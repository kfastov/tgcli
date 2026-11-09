#!/usr/bin/env node
// Writes docs/mcp-tools.md from the operation declarations.

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

import { OPERATION_LIST } from '../core/ops/index.js';
import { renderMcpToolsDoc } from '../core/ops/docs.js';

const target = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'docs', 'mcp-tools.md');
fs.writeFileSync(target, renderMcpToolsDoc(OPERATION_LIST));
console.log(`Wrote ${path.relative(process.cwd(), target)}`);
