// Storage format for `messages.raw_json`, the archived copy of the serialized
// message including mtcute's raw TL object.
//
// A plain JSON.stringify of a TL object is ~1.5x larger than it needs to be and
// loses binary data: Uint8Array has no toJSON, so photo thumbnails and
// fileReference come out as {"0":1,"1":24,...} index maps (~9 chars per byte)
// that no longer read back as bytes.
//
// serializeRawJson:
//   - binary (Uint8Array, Buffer) -> {"$b64":"<base64>"}, lossless;
//   - null object properties are omitted (null is never a TL value). `false` is
//     kept: for `flags.N?Bool` fields (e.g. messageActionTopicEdit.closed) an
//     explicit false means something different from an absent field;
//   - array elements are left alone so positions stay intact.
// parseRawJson restores {"$b64"} values and legacy index maps to Uint8Array, so
// rows written before this format still rebuild correct media file ids.

const BASE64_KEY = '$b64';

function isNodeBufferJson(value) {
  return value !== null
    && typeof value === 'object'
    && value.type === 'Buffer'
    && Array.isArray(value.data)
    && Object.keys(value).length === 2;
}

function encodeBytes(data) {
  return { [BASE64_KEY]: Buffer.from(data).toString('base64') };
}

function replacer(key, value) {
  // Buffer#toJSON runs before the replacer and yields {type:'Buffer', data:[...]}.
  if (value instanceof Uint8Array || isNodeBufferJson(value)) {
    return encodeBytes(value instanceof Uint8Array ? value : value.data);
  }
  if (value === null && key !== '' && !Array.isArray(this)) {
    return undefined;
  }
  return value;
}

export function serializeRawJson(value) {
  return JSON.stringify(value, replacer);
}

function isLegacyByteMap(value) {
  const keys = Object.keys(value);
  if (keys.length === 0) {
    return false;
  }
  for (let i = 0; i < keys.length; i += 1) {
    if (keys[i] !== String(i)) {
      return false;
    }
    const byte = value[keys[i]];
    if (!Number.isInteger(byte) || byte < 0 || byte > 255) {
      return false;
    }
  }
  return true;
}

function reviver(_key, value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return value;
  }
  const encoded = value[BASE64_KEY];
  if (typeof encoded === 'string' && Object.keys(value).length === 1) {
    return Uint8Array.from(Buffer.from(encoded, 'base64'));
  }
  if (isLegacyByteMap(value)) {
    return Uint8Array.from(Object.values(value));
  }
  return value;
}

export function parseRawJson(text) {
  if (!text || typeof text !== 'string') {
    return null;
  }
  try {
    return JSON.parse(text, reviver);
  } catch {
    return null;
  }
}

// Re-encodes a stored raw_json value in the current format. Returns null when
// the input cannot be parsed, so callers leave such rows untouched.
export function compactRawJson(text) {
  const parsed = parseRawJson(text);
  return parsed === null ? null : serializeRawJson(parsed);
}
