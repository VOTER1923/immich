import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:http/http.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/services/api.service.dart';
import 'package:logging/logging.dart';
import 'package:path_provider/path_provider.dart';

/// Hard-coded settings: any file larger than this goes through the tus service.
const int kTusThresholdBytes = 8 * 1024 * 1024;
const int kTusChunkBytes = 32 * 1024 * 1024;
const String kTusEndpoint = 'https://upload.alexnet.com.au/files/';

class TusResult {
  final String? remoteAssetId;
  final int? statusCode;
  final String? errorMessage;
  final bool cancelled;
  const TusResult({this.remoteAssetId, this.statusCode, this.errorMessage, this.cancelled = false});
  bool get isSuccess => remoteAssetId != null;
}

/// Minimal tus 1.0.0 client (create, resume via HEAD, PATCH in chunks).
class TusUploader {
  static final Logger _log = Logger('TusUploader');

  static Future<File> _storeFile() async {
    final dir = await getApplicationSupportDirectory();
    return File('${dir.path}/tus_uploads.json');
  }

  static Future<Map<String, dynamic>> _readStore() async {
    try {
      final f = await _storeFile();
      if (await f.exists()) return jsonDecode(await f.readAsString()) as Map<String, dynamic>;
    } catch (_) {}
    return {};
  }

  static Future<void> _writeStore(Map<String, dynamic> m) async {
    try {
      await (await _storeFile()).writeAsString(jsonEncode(m));
    } catch (_) {}
  }

  static Future<void> _remember(String key, String? location) async {
    final m = await _readStore();
    if (location == null) {
      m.remove(key);
    } else {
      m[key] = location;
    }
    await _writeStore(m);
  }

  static Map<String, String> _baseHeaders() {
    final token = Store.tryGet(StoreKey.accessToken);
    return {
      'Tus-Resumable': '1.0.0',
      ...ApiService.getRequestHeaders(),
      if (token != null) 'x-immich-user-token': token,
    };
  }

  static String _encodeMetadata(Map<String, String> m) => m.entries
      .where((e) => e.value.isNotEmpty)
      .map((e) => '${e.key} ${base64.encode(utf8.encode(e.value))}')
      .join(',');

  static Future<StreamedResponse> _send(Client client, BaseRequest r) => client.send(r);

  /// Returns the server's current offset, or null if the upload no longer exists.
  static Future<int?> _head(Client client, Uri location, Completer<void>? cancel) async {
    final req = AbortableRequest('HEAD', location, abortTrigger: cancel?.future)..headers.addAll(_baseHeaders());
    final res = await _send(client, req);
    await res.stream.drain();
    if (res.statusCode == 200 || res.statusCode == 204) {
      return int.tryParse(res.headers['upload-offset'] ?? '');
    }
    return null;
  }

  static Future<TusResult> upload({
    required Client client,
    required File file,
    required String originalFileName,
    required Map<String, String> fields,
    required Completer<void>? cancelToken,
    void Function(int bytes, int totalBytes)? onProgress,
  }) async {
    final length = file.lengthSync();
    final key = '${fields['deviceAssetId'] ?? originalFileName}|$length';
    final endpoint = Uri.parse(kTusEndpoint);

    try {
      Uri? location;
      int offset = 0;

      // Try to resume a previous attempt.
      final saved = (await _readStore())[key] as String?;
      if (saved != null) {
        final u = Uri.parse(saved);
        final off = await _head(client, u, cancelToken);
        if (off != null && off < length) {
          location = u;
          offset = off;
        } else {
          // Gone, or finished earlier without us receiving the asset id: start fresh.
          await _remember(key, null);
        }
      }

      // Create a new upload.
      if (location == null) {
        final req = AbortableRequest('POST', endpoint, abortTrigger: cancelToken?.future)
          ..headers.addAll(_baseHeaders())
          ..headers['Upload-Length'] = '$length'
          ..headers['Upload-Metadata'] = _encodeMetadata({...fields, 'filename': originalFileName});
        final res = await _send(client, req);
        await res.stream.drain();
        if (res.statusCode != 201) {
          return TusResult(statusCode: res.statusCode, errorMessage: 'Chunk service refused upload (${res.statusCode})');
        }
        final loc = res.headers['location'];
        if (loc == null) return const TusResult(errorMessage: 'Chunk service gave no upload location');
        location = endpoint.resolve(loc);
        offset = 0;
        await _remember(key, location.toString());
      }

      // Send chunks.
      while (offset < length) {
        final chunkStart = offset;
        final end = (offset + kTusChunkBytes) > length ? length : offset + kTusChunkBytes;
        final builder = BytesBuilder(copy: false);
        await for (final part in file.openRead(chunkStart, end)) {
          builder.add(part);
        }
        final bytes = builder.takeBytes();

        StreamedResponse? res;
        Object? lastError;
        for (var attempt = 0; attempt < 5; attempt++) {
          try {
            final req = AbortableRequest('PATCH', location, abortTrigger: cancelToken?.future)
              ..headers.addAll(_baseHeaders())
              ..headers['Content-Type'] = 'application/offset+octet-stream'
              ..headers['Upload-Offset'] = '$chunkStart'
              ..bodyBytes = bytes;
            res = await _send(client, req);
            break;
          } on RequestAbortedException {
            rethrow;
          } catch (e) {
            lastError = e;
            _log.warning('Chunk at $chunkStart failed (attempt ${attempt + 1}): $e');
            await Future.delayed(Duration(seconds: 2 * (attempt + 1)));
            try {
              final off = await _head(client, location, cancelToken);
              if (off != null && off != chunkStart) {
                offset = off; // server's view differs (e.g. it got the chunk); resync
                break;
              }
            } on RequestAbortedException {
              rethrow;
            } catch (_) {}
          }
        }

        if (res == null) {
          if (offset != chunkStart) {
            onProgress?.call(offset, length);
            continue;
          }
          return TusResult(errorMessage: 'Chunk upload failed: $lastError');
        }

        final body = await res.stream.bytesToString();
        if (res.statusCode != 200 && res.statusCode != 204) {
          return TusResult(statusCode: res.statusCode, errorMessage: 'Chunk service error ${res.statusCode}: $body');
        }
        offset = int.tryParse(res.headers['upload-offset'] ?? '') ?? end;
        onProgress?.call(offset, length);

        if (offset >= length) {
          // Final response carries Immich's JSON (set by the server's pre-finish hook).
          await _remember(key, null);
          try {
            final id = (jsonDecode(body) as Map<String, dynamic>)['id'] as String?;
            if (id != null) return TusResult(remoteAssetId: id);
          } catch (_) {}
          return TusResult(statusCode: res.statusCode, errorMessage: 'Upload finished but server returned no asset id');
        }
      }
      // Server already had every byte but we never saw the final response; next run starts fresh.
      return const TusResult(errorMessage: 'Upload completed but no asset id was received');
    } on RequestAbortedException {
      return const TusResult(cancelled: true);
    } catch (e, st) {
      _log.warning('tus upload failed', e, st);
      return TusResult(errorMessage: e.toString());
    }
  }
}
