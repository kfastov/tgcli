// Builds the CLI commands of the operations from their declarations. A command
// reads its options into the operation's args, validates them with the
// operation's schema (errors name the options), runs the caller-side prepare
// hook, asks the server to run the operation, and prints the payload as JSON or
// through the operation's renderer.

import { Option } from 'commander';

import { OperationInputError, parseOperationArgs } from './operation.js';
import { withGlobalOptions, writeJson } from '../cli-io.js';

const LIST_KINDS = new Set(['list', 'intList']);

function collect(value, previous) {
  return (previous ?? []).concat(value);
}

function lowerFirst(text) {
  return text ? `${text[0].toLowerCase()}${text.slice(1)}` : text;
}

function optionHelp(field, help) {
  const text = help ?? field.help ?? '';
  if (field.defaultValue === undefined || field.kind === 'bool') {
    return text;
  }
  return `${text} (default: ${[field.defaultValue].flat().join(',')})`;
}

// The commander options of one field: its flag, the '--no-' twin of a negatable
// boolean, and its aliases. Returns them so the field can be read back.
function addFieldOptions(command, field) {
  const { flag, help, aliases = [], negatable } = field.cli;
  if (!flag) {
    return [];
  }
  const list = LIST_KINDS.has(field.kind);
  const options = [];
  const main = new Option(flag, optionHelp(field, help));
  if (list) main.argParser(collect);
  options.push(main);
  if (negatable) {
    options.push(new Option(`--no-${flag.slice(2)}`, `Do not ${lowerFirst(help ?? field.help)}`));
  }
  for (const alias of aliases) {
    const option = new Option(alias.flag, alias.help ?? optionHelp(field, help));
    if (alias.hidden) option.hideHelp();
    if (list) option.argParser(collect);
    options.push(option);
  }
  for (const option of options) {
    command.addOption(option);
  }
  return options;
}

function splitList(values) {
  return values
    .flatMap((entry) => String(entry).split(','))
    .map((entry) => entry.trim())
    .filter(Boolean);
}

// A number the way the schema wants it, or the string as given, which the schema
// then rejects with the option's name.
function toNumber(value) {
  const text = String(value).trim();
  return /^-?\d+$/.test(text) ? Number(text) : value;
}

function coerce(field, value) {
  if (field.kind === 'int') return toNumber(value);
  if (field.kind === 'choice') return String(value).toLowerCase();
  if (field.kind === 'intList') return value.map(toNumber);
  return value;
}

function readBool(field, [main], opts) {
  const value = opts[main.attributeName()];
  if (field.cli.negatable) {
    return value;
  }
  // '--no-x' alone is stored by commander as x: false.
  const given = main.negate ? value === false : value === true;
  return given ? field.cli.presence : undefined;
}

function readField(field, options, opts, positionalValue) {
  if (field.kind === 'bool') {
    return options.length ? readBool(field, options, opts) : undefined;
  }
  if (LIST_KINDS.has(field.kind)) {
    const given = options.map((option) => opts[option.attributeName()]).filter((value) => value !== undefined);
    return given.length ? coerce(field, splitList(given.flat())) : undefined;
  }
  // A blank option ('--limit ""', '--after "$UNSET"') counts as not given, unless
  // the field takes blank text ('--notes ""' clears the notes); it also leaves the
  // positional it shares the field with.
  const blankMeansUnset = !field.schema.safeParse('').success || positionalValue?.length;
  const given = options
    .map((option) => ({ option, value: opts[option.attributeName()] }))
    .filter(({ value }) => value !== undefined && !(blankMeansUnset && String(value).trim() === ''));
  const distinct = [...new Set(given.map(({ value }) => coerce(field, value)))];
  if (distinct.length > 1) {
    const names = given.map(({ option }) => option.long).join(' and ');
    throw new OperationInputError(`${names} must match when both are provided`);
  }
  if (distinct.length) {
    return distinct[0];
  }
  if (positionalValue === undefined) {
    return undefined;
  }
  const text = [positionalValue].flat().join(' ').trim();
  return text ? coerce(field, text) : undefined;
}

async function runFromCli(op, readArgs, globalFlags, invoke) {
  // How long the caller waits for the server: --timeout, else the operation's own
  // default, else the control client's.
  const timeoutMs = globalFlags.timeoutMs ?? op.client.timeoutMs;
  const caller = { cwd: process.cwd(), timeoutMs };
  let args;
  let payload;
  try {
    args = parseOperationArgs(op, readArgs(), 'cli');
    if (op.client.prepare) {
      args = op.client.prepare(args, caller);
    }
    payload = await invoke(globalFlags, { op: op.name, args, invokeTimeoutMs: timeoutMs });
  } catch (error) {
    throw op.client.mapError ? op.client.mapError(error, { args, timeoutMs }) : error;
  }
  if (globalFlags.json) {
    writeJson(payload);
  } else {
    op.render(payload, {
      args,
      print: (line) => console.log(line),
      warn: (line) => console.error(line),
    });
  }
}

// Adds a command for every operation that has one. groups: help for the parent
// commands ('groups members'); invoke(globalFlags, { op, args, invokeTimeoutMs })
// asks the server to run an operation and returns its payload.
export function addOperationCommands(program, { operations, groups = {}, invoke }) {
  const parents = new Map([['', program]]);
  const parentOf = (names) => {
    const key = names.join(' ');
    if (!parents.has(key)) {
      const parent = parentOf(names.slice(0, -1));
      const existing = parent.commands.find((command) => command.name() === names.at(-1));
      parents.set(key, existing ?? parent.command(names.at(-1)).description(groups[key] ?? ''));
    }
    return parents.get(key);
  };

  for (const op of operations) {
    if (!op.command) {
      continue;
    }
    const names = op.command.split(' ');
    const command = parentOf(names.slice(0, -1))
      .command(names.at(-1), { hidden: op.hidden })
      .description(op.summary);
    const fields = Object.entries(op.input);
    const positional = fields.find(([, field]) => field.cli.positional);
    if (positional) {
      command.argument(positional[1].cli.positional, positional[1].help);
    }
    const optionsOf = new Map(fields.map(([key, field]) => [key, addFieldOptions(command, field)]));
    command.action(withGlobalOptions((globalFlags, ...actionArgs) => {
      const opts = actionArgs.at(-1).opts();
      const positionalValue = positional ? actionArgs[0] : undefined;
      const readArgs = () => {
        const raw = {};
        for (const [key, field] of fields) {
          const value = readField(field, optionsOf.get(key), opts, key === positional?.[0] ? positionalValue : undefined);
          if (value !== undefined) {
            raw[key] = value;
          }
        }
        return raw;
      };
      return runFromCli(op, readArgs, globalFlags, invoke);
    }));
  }
}
