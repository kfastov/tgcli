// An operation is declared once: its input fields (a zod schema each, plus how the
// CLI spells it), its MCP annotations, the caller-side hooks, the handler the server
// runs, and the human rendering. The CLI commands, the MCP tools and the control API
// all read the same declaration (docs/adr/0002-operations-declared-once.md).

import { z } from 'zod';

// A chat or a user as the caller names it: a numeric id (any sign) or a username.
const peerRef = z.union([z.number().int(), z.string().trim().min(1)]);

// One input field: the zod schema that validates it on every surface, what a
// valid value looks like (for error messages), and its CLI spelling.
class Field {
  constructor({ kind, schema, expects, help, cli = {}, defaultValue }) {
    this.kind = kind;
    this.schema = schema;
    this.expects = expects;
    this.help = help;
    this.cli = cli;
    this.defaultValue = defaultValue;
    Object.freeze(this);
  }

  #with(patch) {
    return new Field({ ...this, ...patch });
  }

  optional() {
    return this.#with({ schema: this.schema.optional() });
  }

  default(value) {
    return this.#with({ schema: this.schema.default(value), defaultValue: value });
  }

  // A CLI option, such as '--chat <id|username>' or '--silent'. Options:
  //   help:      CLI help text when it differs from the field's description;
  //   aliases:   alternative spellings, hidden unless given as { flag, help }; a list
  //              field gathers the values of all of them, any other field takes the
  //              one value they agree on;
  //   presence:  for a boolean option, the value the field takes when the option is
  //              given (true by default; '--no-refresh-metadata' gives false);
  //   negatable: a boolean that also gets '--no-<name>', leaving the field unset when
  //              neither is given.
  flag(spec, { help, aliases = [], presence = true, negatable = false } = {}) {
    const normalized = aliases.map((alias) => (typeof alias === 'string' ? { flag: alias, hidden: true } : alias));
    return this.#with({ cli: { ...this.cli, flag: spec, help, aliases: normalized, presence, negatable } });
  }

  // A CLI positional argument, such as '<folder>' or '[query...]'. A variadic one
  // is joined with spaces. With a flag too, the flag wins when given.
  positional(spec) {
    return this.#with({ cli: { ...this.cli, positional: spec } });
  }
}

function describeIntRange(min, max) {
  const lower = min === 0 ? 'a non-negative integer' : min === 1 ? 'a positive integer' : `an integer of at least ${min}`;
  return max === undefined ? lower : `${lower} up to ${max}`;
}

function make(kind, schema, expects, help) {
  return new Field({ kind, schema: help ? schema.describe(help) : schema, expects, help });
}

// The field types operations take. The kind tells the CLI how to read a string.
export const field = {
  chat: (help) => make('string', peerRef, 'a chat id or username', help),
  // One chat or several; a CLI option is repeatable and takes comma-separated values.
  chats: (help) => make('list', z.union([peerRef, z.array(peerRef)]), 'chat ids or usernames', help),
  user: (help) => make('string', peerRef, 'a user id or username', help),
  users: (help) => make('list', z.array(peerRef).min(1), 'user ids or usernames', help),
  folder: (help) => make('string', peerRef, 'a folder id or title', help),
  text: (help) => make('string', z.string().min(1), 'non-empty text', help),
  textOrEmpty: (help) => make('string', z.string(), 'text', help),
  // An ISO-8601 date or datetime.
  date: (help) => make(
    'string',
    z.string().min(1).refine((value) => !Number.isNaN(Date.parse(value))),
    'an ISO-8601 date',
    help,
  ),
  int: (help, { min = 1, max } = {}) => make(
    'int',
    max === undefined ? z.number().int().min(min) : z.number().int().min(min).max(max),
    describeIntRange(min, max),
    help,
  ),
  ints: (help) => make('intList', z.array(z.number().int()).min(1), 'a list of integers', help),
  bool: (help) => make('bool', z.boolean(), 'true or false', help),
  choice: (values, help) => make('choice', z.enum(values), `one of: ${values.join(', ')}`, help),
  strings: (help) => make('list', z.array(z.string().trim().min(1)), 'a list of names', help),
};

// A payload carries dates as ISO strings: run returns JSON-safe data.
export function toIsoDate(value) {
  return value instanceof Date ? value.toISOString() : (value ?? null);
}

export class OperationInputError extends Error {
  constructor(message) {
    super(message);
    this.name = 'OperationInputError';
  }
}

// declaration:
//   name         wire name of the control API, and the MCP tool name;
//   command      CLI path ('groups members add'), or null for an MCP-only operation;
//   hidden       keep the CLI command out of help;
//   mcp          false keeps the operation off MCP;
//   summary      one line for the CLI help; description (default: summary) for MCP;
//   annotations  { readOnly, destructive, idempotent } hints for MCP clients;
//   telegram     whether the server logs in before running it: true (default),
//                false, or a function of the parsed args;
//   input        { key: Field };
//   refine       (args, fail) => void, checks across fields; fail(message) where the
//                message names fields as {key};
//   client       caller-side hooks: prepare(args, caller) returns the args to send
//                (caller: { cwd, timeoutMs }, timeoutMs being how long the caller
//                waits); timeoutMs, the default wait; mapError(error, { args,
//                timeoutMs }) for errors on the caller's side, validation included;
//   run          async (services, args) => payload, JSON-safe; the payload is what
//                --json prints and what the MCP tool returns;
//   render       (payload, { args, print, warn }) => void, the human CLI output
//                (default: the payload as indented JSON).
export function defineOperation(declaration) {
  const {
    name,
    command = null,
    hidden = false,
    mcp = true,
    summary,
    description = summary,
    annotations = {},
    telegram = true,
    input = {},
    refine = null,
    client = {},
    run,
    render = renderJson,
  } = declaration;
  if (!name || typeof run !== 'function') {
    throw new Error(`Operation ${name ?? '(unnamed)'} needs a name and a run handler`);
  }
  const positionals = Object.values(input).filter((value) => value.cli.positional);
  if (positionals.length > 1) {
    throw new Error(`Operation ${name} takes one positional argument at most`);
  }
  if (Object.values(input).some((value) => value.kind === 'bool' && value.cli.aliases?.length)) {
    throw new Error(`Operation ${name}: a boolean option takes no aliases`);
  }
  const shape = Object.fromEntries(Object.entries(input).map(([key, value]) => [key, value.schema]));
  const objectSchema = z.strictObject(shape);
  const schema = refine
    ? objectSchema.superRefine((args, ctx) => {
      refine(args, (message) => ctx.addIssue({ code: 'custom', path: [], message }));
    })
    : objectSchema;
  return Object.freeze({
    name,
    command,
    hidden,
    mcp: Boolean(mcp),
    summary,
    description,
    annotations,
    telegram,
    input,
    objectSchema,
    schema,
    client,
    run,
    render,
  });
}

export function needsTelegram(op, args) {
  return typeof op.telegram === 'function' ? Boolean(op.telegram(args)) : op.telegram !== false;
}

// The name a surface gives a field: 'cli' spells it as its option ('--before-id') or
// positional ('folder'); 'wire' (the control API and MCP) as its key.
function spellField(op, key, surface) {
  if (surface !== 'cli') {
    return key;
  }
  const cli = op.input[key]?.cli ?? {};
  if (cli.flag) {
    return cli.flag.split(/[\s,]/)[0];
  }
  if (cli.positional) {
    return cli.positional.replace(/[<>[\].]/g, '');
  }
  return key;
}

// Validates raw args against the operation, returning them with defaults applied.
// Throws OperationInputError naming each problem the way the surface spells it.
export function parseOperationArgs(op, raw, surface) {
  const result = op.schema.safeParse(raw ?? {}, { reportInput: true });
  if (result.success) {
    return result.data;
  }
  const messages = [...new Set(result.error.issues.map((issue) => describeIssue(op, issue, surface)))];
  throw new OperationInputError(messages.join('; '));
}

function describeIssue(op, issue, surface) {
  const spell = (key) => spellField(op, key, surface);
  if (issue.code === 'custom' && issue.path.length === 0) {
    return issue.message.replace(/\{(\w+)\}/g, (_, key) => spell(key));
  }
  if (issue.code === 'unrecognized_keys') {
    const keys = issue.keys.map(spell).join(', ');
    return `${op.name} does not take ${keys}`;
  }
  const key = issue.path[0];
  if (key === undefined) {
    return issue.message;
  }
  if (issue.path.length === 1 && issue.input === undefined) {
    return `${spell(key)} is required`;
  }
  return `${spell(key)} must be ${op.input[key]?.expects ?? 'valid'}`;
}

function renderJson(payload, { print }) {
  print(JSON.stringify(payload, null, 2));
}
