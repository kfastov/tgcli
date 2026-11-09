// docs/mcp-tools.md is written from the declarations; this fails when they move
// apart. Fix it with `node scripts/gen-mcp-tools-doc.js`.

import fs from 'fs';
import { describe, expect, it } from 'vitest';

import { OPERATION_LIST } from '../core/ops/index.js';
import { renderMcpToolsDoc } from '../core/ops/docs.js';

describe('docs/mcp-tools.md', () => {
  it('matches the operation declarations', () => {
    const doc = fs.readFileSync(new URL('../docs/mcp-tools.md', import.meta.url), 'utf8');
    expect(doc).toBe(renderMcpToolsDoc(OPERATION_LIST));
  });
});
