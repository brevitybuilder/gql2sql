import b from 'benny'
import { parse, print } from 'graphql'

import { gql2Sql } from '../index'

function parsejs(query: string) {
  return parse(query)
}

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
`

async function run() {
  const buf = new TextEncoder().encode(query);
  await b.suite(
    'graphql',
    b.add('Native parse and convert and print', () => {
      const result = gql2Sql(buf)
      new TextDecoder().decode(result)
    }),
    b.add('JavaScript parse and print', () => {
      const ast = parsejs(query)
      print(ast)
    }),
    b.cycle(),
    b.complete(),
  )
}

run().catch((e) => {
  console.error(e)
})
