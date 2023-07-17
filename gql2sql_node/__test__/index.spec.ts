import test from 'ava'

import { gql2Sql } from '../index'

const gql = String.raw

test('sync function from native code', (t) => {
  const fixture = gql`
    query App {
      App(filter: { field: "id", operator: "eq", value: "345810043118026832" }, order: { name: ASC }) {
        id
        components @relation(table: "Component", field: ["appId"], references: ["id"]) {
          id
          pageMeta @relation(table: "PageMeta", field: ["componentId"], references: ["id"], single: true) {
            id
            path
          }
          elements(order: { order: ASC })
            @relation(table: "Element", field: ["componentParentId"], references: ["id"]) {
            id
            name
          }
        }
      }
      Component_aggregate(filter: { field: "appId", operator: "eq", value: "345810043118026832" }) {
        count
        min {
          createdAt
        }
      }
    }
  `
  const result = gql2Sql(JSON.stringify({ query: fixture, variables: {} }))
  t.snapshot(JSON.parse(result))
})

test('complex query', (t) => {
  const fixture = gql`
    query GetApp($orgId: String!, $appId: String!, $branch: String!) {
      app: App_one(
        filter: {
          field: "orgId"
          operator: "eq"
          value: $orgId
          logicalOperator: "AND"
          children: [
            { field: "id", operator: "eq", value: $appId }
            { field: "branch", operator: "eq", value: $branch }
          ]
        }
      ) {
        orgId
        id
        branch
        name
        description
        theme
        favicon
        customCSS
        analytics
        customDomain
        components @relation(table: "Component", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          ... on PageMeta
            @relation(table: "PageMeta", field: ["componentId", "branch"], references: ["id", "branch"], single: true) {
            title
            description
            path
            socialImage
            urlParams
            loader
            protection
            maxAge
            sMaxAge
            staleWhileRevalidate
          }
          ... on ComponentMeta
            @relation(
              table: "ComponentMeta"
              field: ["componentId", "branch"]
              references: ["id", "branch"]
              single: true
            ) {
            title
            sources @relation(table: "Source", field: ["componentId", "branch"], references: ["id", "branch"]) {
              id
              branch
              name
              provider
              description
              template
              instanceTemplate
              outputType
              source
              sourceProp
              componentId
              utilityId
              component(order: { order: ASC })
                @relation(
                  table: "Element"
                  field: ["id", "branch"]
                  references: ["componentId", "branch"]
                  single: true
                ) {
                id
                branch
                name
                kind
                source
                styles
                props
                order
                conditions
              }
              utility
                @relation(
                  table: "Utility"
                  field: ["id", "branch"]
                  references: ["componentId", "branch"]
                  single: true
                ) {
                id
                branch
                name
                kind
                kindId
                data
              }
            }
            events @relation(table: "Event", field: ["componentMetaId", "branch"], references: ["id", "branch"]) {
              id
              branch
              name
              label
              help
              type
            }
          }
        }
        connections @relation(table: "Connection", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          kind
          prodUrl
          mutationSchema
            @relation(
              table: "Schema"
              field: ["mutationConnectionId", "branch"]
              references: ["id", "branch"]
              single: true
            ) {
            id
            branch
            schema
          }
          endpoints @relation(table: "Endpoint", field: ["connectionId", "branch"], references: ["id", "branch"]) {
            id
            branch
            name
            method
            path
            responseSchemaId
            headers @relation(table: "Header", field: ["parentEndpointId", "branch"], references: ["id", "branch"]) {
              id
              branch
              key
              value
              dynamic
            }
            search @relation(table: "Search", field: ["endpointId", "branch"], references: ["id", "branch"]) {
              id
              branch
              key
              value
              dynamic
            }
          }
          headers @relation(table: "Header", field: ["parentConnectionId", "branch"], references: ["id", "branch"]) {
            id
            branch
            key
            value
            dynamic
          }
        }
        layouts @relation(table: "Layout", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          source
          kind
          styles
          props
        }
        plugins @relation(table: "Plugin", field: ["appId", "branch"], references: ["id", "branch"]) {
          instanceId
          kind
        }
        schemas @relation(table: "Schema", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          schema
        }
        styles @relation(table: "Style", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          kind
          styles
          isDefault
        }
        workflows @relation(table: "Workflow", field: ["appId", "branch"], references: ["id", "branch"]) {
          id
          branch
          name
          args
          steps(order: { order: ASC })
            @relation(table: "Step", field: ["workflowId", "branch"], references: ["id", "branch"]) {
            id
            branch
            parentId
            kind
            kindId
            data
            order
          }
        }
      }
    }
  `
  const result = gql2Sql(
    JSON.stringify({
      query: fixture,
      variables: {
        orgId: '123',
        appId: '456',
        branch: 'master',
      },
    }),
  )
  t.snapshot(JSON.parse(result))
})
