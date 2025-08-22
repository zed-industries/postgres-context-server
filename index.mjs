#!/usr/bin/env node

import pg from "pg";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListPromptsRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
  GetPromptRequestSchema,
  CompleteRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

const server = new Server({
  name: "postgres-context-server",
  version: "0.1.0",
});

const databaseUrl = process.env.DATABASE_URL;
if (typeof databaseUrl == null || databaseUrl.trim().length === 0) {
  console.error("Please provide a DATABASE_URL environment variable");
  process.exit(1);
}

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

process.stderr.write("starting server\n");

const pool = new pg.Pool({
  connectionString: databaseUrl,
});

const SCHEMA_PATH = "schema";
const SCHEMA_PROMPT_NAME = "pg-schema";
const ALL_TABLES = "all-tables";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
      [tableName],
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "pg-schema",
        description: "Returns the schema for a Postgres database.",
        inputSchema: {
          type: "object",
          properties: {
            mode: {
              type: "string",
              enum: ["all", "specific"],
              description: "Mode of schema retrieval",
            },
            tableName: {
              type: "string",
              description:
                "Name of the specific table (required if mode is 'specific')",
            },
          },
          required: ["mode"],
          if: {
            properties: { mode: { const: "specific" } },
          },
          then: {
            required: ["tableName"],
          },
        },
      },
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "pg-schema") {
    const mode = request.params.arguments?.mode;

    const tableName = (() => {
      switch (mode) {
        case "specific": {
          const tableName = request.params.arguments?.tableName;

          if (typeof tableName !== "string" || tableName.length === 0) {
            throw new Error(`Invalid tableName: ${tableName}`);
          }

          return tableName;
        }
        case "all": {
          return ALL_TABLES;
        }
        default:
          throw new Error(`Invalid mode: ${mode}`);
      }
    })();

    const client = await pool.connect();

    try {
      const sql = await getSchema(client, tableName);

      return {
        content: [{ type: "text", text: sql }],
      };
    } finally {
      client.release();
    }
  }

  if (request.params.name === "query") {
    const sql = request.params.arguments?.sql;

    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      // Force a prepared statement: Prevents multiple statements in the same query.
      // Name is unique per session, but we use a single session per query.
      const result = await client.query({
        name: "sandboxed-statement",
        text: sql,
        values: [],
      });
      return {
        content: [
          { type: "text", text: JSON.stringify(result.rows, undefined, 2) },
        ],
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error),
        );

      // Destroy session to clean up resources.
      client.release(true);
    }
  }

  throw new Error("Tool not found");
});

server.setRequestHandler(CompleteRequestSchema, async (request) => {
  process.stderr.write("Handling completions/complete request\n");

  if (request.params.ref.name === SCHEMA_PROMPT_NAME) {
    const tableNameQuery = request.params.argument.value;
    const alreadyHasArg = /\S*\s/.test(tableNameQuery);

    if (alreadyHasArg) {
      return {
        completion: {
          values: [],
        },
      };
    }

    const client = await pool.connect();
    try {
      const result = await client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
      );
      const tables = result.rows.map((row) => row.table_name);
      return {
        completion: {
          values: [ALL_TABLES, ...tables],
        },
      };
    } finally {
      client.release();
    }
  }

  throw new Error("unknown prompt");
});

server.setRequestHandler(ListPromptsRequestSchema, async () => {
  process.stderr.write("Handling prompts/list request\n");

  return {
    prompts: [
      {
        name: SCHEMA_PROMPT_NAME,
        description:
          "Retrieve the schema for a given table in the postgres database",
        arguments: [
          {
            name: "tableName",
            description: "the table to describe",
            required: true,
          },
        ],
      },
    ],
  };
});

server.setRequestHandler(GetPromptRequestSchema, async (request) => {
  process.stderr.write("Handling prompts/get request\n");

  if (request.params.name === SCHEMA_PROMPT_NAME) {
    const tableName = request.params.arguments?.tableName;

    if (typeof tableName !== "string" || tableName.length === 0) {
      throw new Error(`Invalid tableName: ${tableName}`);
    }

    const client = await pool.connect();

    try {
      const sql = await getSchema(client, tableName);

      return {
        description:
          tableName === ALL_TABLES
            ? "all table schemas"
            : `${tableName} schema`,
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: sql,
            },
          },
        ],
      };
    } finally {
      client.release();
    }
  }

  throw new Error(`Prompt '${request.params.name}' not implemented`);
});

/**
 * @param tableNameOrAll {string}
 */
async function getSchema(client, tableNameOrAll) {
  const select =
    "SELECT column_name, data_type, is_nullable, column_default, table_name FROM information_schema.columns";

  let result;
  if (tableNameOrAll === ALL_TABLES) {
    result = await client.query(
      `${select} WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`,
    );
  } else {
    result = await client.query(`${select} WHERE table_name = $1`, [
      tableNameOrAll,
    ]);
  }

  const allTableNames = Array.from(
    new Set(result.rows.map((row) => row.table_name).sort()),
  );

  let sql = "```sql\n";
  for (let i = 0, len = allTableNames.length; i < len; i++) {
    const tableName = allTableNames[i];
    if (i > 0) {
      sql += "\n";
    }

    sql += [
      `create table "${tableName}" (`,
      result.rows
        .filter((row) => row.table_name === tableName)
        .map((row) => {
          const notNull = row.is_nullable === "NO" ? "" : " not null";
          const defaultValue =
            row.column_default != null ? ` default ${row.column_default}` : "";
          return `    "${row.column_name}" ${row.data_type}${notNull}${defaultValue}`;
        })
        .join(",\n"),
      ");",
    ].join("\n");
    sql += "\n";
  }
  sql += "```";

  return sql;
}

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch((error) => {
  console.error(error);
  process.exit(1);
});
