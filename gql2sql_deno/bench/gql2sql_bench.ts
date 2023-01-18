import { gql2sql } from "../bindings/bindings.ts";
import {
  parse,
  print,
} from "https://raw.githubusercontent.com/adelsz/graphql-deno/v15.0.0/mod.ts";

const query = `
query App {
    App(filter: { id: { eq: "345810043118026832" } }, order: { name: ASC }) {
        id
        components @relation(table: "Component", field: ["appId"], references: ["id"]) {
            id
            pageMeta @relation(table: "PageMeta", field: ["componentId"], references: ["id"], single: true) {
              id
              path
            }
            elements(order: { order: ASC }) @relation(table: "Element", field: ["componentParentId"], references: ["id"]) {
                id
                name
            }
        }
    }
    Component_aggregate(filter: { appId: { eq: "345810043118026832" } }) {
      count
      min {
        createdAt
      }
    }
}
`;

gql2sql(query);

Deno.bench("Native parse + convert + print", { group: "graphql" }, () => {
  gql2sql(query);
});

Deno.bench(
  "JavaScript parse + print",
  { group: "graphql", baseline: true },
  () => {
    const parsed = parse(query);
    print(parsed);
  }
);
