// Registers one MCP tool per operation, with the operation's schema and hints.

// An operation that never needs Telegram works on the local archive alone.
function toolAnnotations(op) {
  const { readOnly = false, destructive = false, idempotent = false } = op.annotations;
  const openWorldHint = op.telegram !== false;
  return readOnly
    ? { readOnlyHint: true, openWorldHint }
    : { readOnlyHint: false, destructiveHint: destructive, idempotentHint: idempotent, openWorldHint };
}

// execute(op, args) runs an operation on the server and returns its payload.
export function registerOperationTools(server, operations, execute) {
  for (const op of operations) {
    if (!op.mcp) {
      continue;
    }
    server.registerTool(
      op.name,
      {
        description: op.description,
        inputSchema: op.objectSchema,
        annotations: toolAnnotations(op),
      },
      async (args) => {
        const payload = await execute(op, args ?? {});
        return { content: [{ type: 'text', text: JSON.stringify(payload ?? null, null, 2) }] };
      },
    );
  }
}
