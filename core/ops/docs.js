// The MCP tool reference in docs/mcp-tools.md, written from the declarations so it
// cannot drift from them (scripts/gen-mcp-tools-doc.js writes it; a test checks it).

function hints({ readOnly, destructive, idempotent }) {
  if (readOnly) return 'read-only';
  return [destructive ? 'destructive' : 'not destructive', idempotent ? 'idempotent' : null].filter(Boolean).join(', ');
}

function cell(text) {
  return String(text ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
}

function paramRows(op) {
  return Object.entries(op.input).map(([key, value]) => {
    const required = !value.schema.safeParse(undefined).success;
    const defaultValue = value.defaultValue === undefined ? '' : `\`${JSON.stringify(value.defaultValue)}\``;
    return `| \`${key}\` | ${cell(value.expects)} | ${required ? 'yes' : 'no'} | ${defaultValue} | ${cell(value.help)} |`;
  });
}

export function renderMcpToolsDoc(operations) {
  const lines = [
    '# MCP tools',
    '',
    '<!-- Generated from core/ops/ by `node scripts/gen-mcp-tools-doc.js`. Do not edit by hand. -->',
    '',
    'MCP is off by default; set `mcp.enabled` to `true` in `config.json` and run `tgcli server`.',
    'Every tool runs the same operation as its CLI command, through the same entry on the server,',
    'and returns the JSON that command prints with `--json`. Params are checked strictly: a param',
    'a tool does not take is an error.',
    '',
  ];
  for (const op of operations.filter((candidate) => candidate.mcp)) {
    lines.push(`## ${op.name}`, '', op.description, '');
    lines.push(`${op.command ? `CLI: \`tgcli ${op.command}\`` : 'MCP only'} · ${hints(op.annotations)}`, '');
    const rows = paramRows(op);
    if (rows.length) {
      lines.push('| Param | Takes | Required | Default | Description |', '|---|---|---|---|---|', ...rows, '');
    } else {
      lines.push('No params.', '');
    }
  }
  return `${lines.join('\n').trimEnd()}\n`;
}
