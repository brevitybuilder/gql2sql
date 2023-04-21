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
  const result = gql2Sql(fixture, {})
  t.is(
    result?.sql,
    `SELECT json_build_object('App', (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data"`,
  )
  t.deepEqual(result.params, undefined)
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
  const expected = `SELECT json_build_object(\'app\', (SELECT to_json((SELECT "root" FROM (SELECT "base"."orgId", "base"."id", "base"."branch", "base"."name", "base"."description", "base"."theme", "base"."favicon", "base"."customCSS", "base"."analytics", "base"."customDomain", "components", "connections", "layouts", "plugins", "schemas", "styles", "workflows") AS "root")) AS "root" FROM (SELECT * FROM "App" WHERE "orgId" = $1 AND "id" = $2 AND "branch" = $3 LIMIT 1) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(CAST(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "base.Component"."branch") AS "root")) AS jsonb) || CASE WHEN "root.PageMeta"."PageMeta" IS NOT NULL THEN to_jsonb("PageMeta") WHEN "root.ComponentMeta"."ComponentMeta" IS NOT NULL THEN to_jsonb("ComponentMeta") ELSE jsonb_build_object() END), \'[]\') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id" AND "Component"."branch" = "base"."branch") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."title", "base.Component.PageMeta"."description", "base.Component.PageMeta"."path", "base.Component.PageMeta"."socialImage", "base.Component.PageMeta"."urlParams", "base.Component.PageMeta"."loader", "base.Component.PageMeta"."protection", "base.Component.PageMeta"."maxAge", "base.Component.PageMeta"."sMaxAge", "base.Component.PageMeta"."staleWhileRevalidate") AS "root")) AS "PageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" AND "PageMeta"."branch" = "base.Component"."branch" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON (\'true\') LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta"."title", "sources", "events") AS "root")) AS "ComponentMeta" FROM (SELECT * FROM "ComponentMeta" WHERE "ComponentMeta"."componentId" = "base.Component"."id" AND "ComponentMeta"."branch" = "base.Component"."branch" LIMIT 1) AS "base.Component.ComponentMeta" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source"."id", "base.Component.ComponentMeta.Source"."branch", "base.Component.ComponentMeta.Source"."name", "base.Component.ComponentMeta.Source"."provider", "base.Component.ComponentMeta.Source"."description", "base.Component.ComponentMeta.Source"."template", "base.Component.ComponentMeta.Source"."instanceTemplate", "base.Component.ComponentMeta.Source"."outputType", "base.Component.ComponentMeta.Source"."source", "base.Component.ComponentMeta.Source"."sourceProp", "base.Component.ComponentMeta.Source"."componentId", "base.Component.ComponentMeta.Source"."utilityId", "component", "utility") AS "root"))), \'[]\') AS "sources" FROM (SELECT * FROM "Source" WHERE "Source"."componentId" = "base.Component.ComponentMeta"."id" AND "Source"."branch" = "base.Component.ComponentMeta"."branch") AS "base.Component.ComponentMeta.Source" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source.Element"."id", "base.Component.ComponentMeta.Source.Element"."branch", "base.Component.ComponentMeta.Source.Element"."name", "base.Component.ComponentMeta.Source.Element"."kind", "base.Component.ComponentMeta.Source.Element"."source", "base.Component.ComponentMeta.Source.Element"."styles", "base.Component.ComponentMeta.Source.Element"."props", "base.Component.ComponentMeta.Source.Element"."order", "base.Component.ComponentMeta.Source.Element"."conditions") AS "root")) AS "component" FROM (SELECT * FROM "Element" WHERE "Element"."id" = "base.Component.ComponentMeta.Source"."componentId" AND "Element"."branch" = "base.Component.ComponentMeta.Source"."branch" ORDER BY "order" ASC LIMIT 1) AS "base.Component.ComponentMeta.Source.Element") AS "root.Element" ON (\'true\') LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source.Utility"."id", "base.Component.ComponentMeta.Source.Utility"."branch", "base.Component.ComponentMeta.Source.Utility"."name", "base.Component.ComponentMeta.Source.Utility"."kind", "base.Component.ComponentMeta.Source.Utility"."kindId", "base.Component.ComponentMeta.Source.Utility"."data") AS "root")) AS "utility" FROM (SELECT * FROM "Utility" WHERE "Utility"."id" = "base.Component.ComponentMeta.Source"."componentId" AND "Utility"."branch" = "base.Component.ComponentMeta.Source"."branch" LIMIT 1) AS "base.Component.ComponentMeta.Source.Utility") AS "root.Utility" ON (\'true\')) AS "root.Source" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Event"."id", "base.Component.ComponentMeta.Event"."branch", "base.Component.ComponentMeta.Event"."name", "base.Component.ComponentMeta.Event"."label", "base.Component.ComponentMeta.Event"."help", "base.Component.ComponentMeta.Event"."type") AS "root"))), \'[]\') AS "events" FROM (SELECT * FROM "Event" WHERE "Event"."componentMetaId" = "base.Component.ComponentMeta"."id" AND "Event"."branch" = "base.Component.ComponentMeta"."branch") AS "base.Component.ComponentMeta.Event") AS "root.Event" ON (\'true\')) AS "root.ComponentMeta" ON (\'true\')) AS "root.Component" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection"."id", "base.Connection"."branch", "base.Connection"."name", "base.Connection"."kind", "base.Connection"."prodUrl", "mutationSchema", "endpoints", "headers") AS "root"))), \'[]\') AS "connections" FROM (SELECT * FROM "Connection" WHERE "Connection"."appId" = "base"."id" AND "Connection"."branch" = "base"."branch") AS "base.Connection" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Connection.Schema"."id", "base.Connection.Schema"."branch", "base.Connection.Schema"."schema") AS "root")) AS "mutationSchema" FROM (SELECT * FROM "Schema" WHERE "Schema"."mutationConnectionId" = "base.Connection"."id" AND "Schema"."branch" = "base.Connection"."branch" LIMIT 1) AS "base.Connection.Schema") AS "root.Schema" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint"."id", "base.Connection.Endpoint"."branch", "base.Connection.Endpoint"."name", "base.Connection.Endpoint"."method", "base.Connection.Endpoint"."path", "base.Connection.Endpoint"."responseSchemaId", "headers", "search") AS "root"))), \'[]\') AS "endpoints" FROM (SELECT * FROM "Endpoint" WHERE "Endpoint"."connectionId" = "base.Connection"."id" AND "Endpoint"."branch" = "base.Connection"."branch") AS "base.Connection.Endpoint" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint.Header"."id", "base.Connection.Endpoint.Header"."branch", "base.Connection.Endpoint.Header"."key", "base.Connection.Endpoint.Header"."value", "base.Connection.Endpoint.Header"."dynamic") AS "root"))), \'[]\') AS "headers" FROM (SELECT * FROM "Header" WHERE "Header"."parentEndpointId" = "base.Connection.Endpoint"."id" AND "Header"."branch" = "base.Connection.Endpoint"."branch") AS "base.Connection.Endpoint.Header") AS "root.Header" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint.Search"."id", "base.Connection.Endpoint.Search"."branch", "base.Connection.Endpoint.Search"."key", "base.Connection.Endpoint.Search"."value", "base.Connection.Endpoint.Search"."dynamic") AS "root"))), \'[]\') AS "search" FROM (SELECT * FROM "Search" WHERE "Search"."endpointId" = "base.Connection.Endpoint"."id" AND "Search"."branch" = "base.Connection.Endpoint"."branch") AS "base.Connection.Endpoint.Search") AS "root.Search" ON (\'true\')) AS "root.Endpoint" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Header"."id", "base.Connection.Header"."branch", "base.Connection.Header"."key", "base.Connection.Header"."value", "base.Connection.Header"."dynamic") AS "root"))), \'[]\') AS "headers" FROM (SELECT * FROM "Header" WHERE "Header"."parentConnectionId" = "base.Connection"."id" AND "Header"."branch" = "base.Connection"."branch") AS "base.Connection.Header") AS "root.Header" ON (\'true\')) AS "root.Connection" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Layout"."id", "base.Layout"."branch", "base.Layout"."name", "base.Layout"."source", "base.Layout"."kind", "base.Layout"."styles", "base.Layout"."props") AS "root"))), \'[]\') AS "layouts" FROM (SELECT * FROM "Layout" WHERE "Layout"."appId" = "base"."id" AND "Layout"."branch" = "base"."branch") AS "base.Layout") AS "root.Layout" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Plugin"."instanceId", "base.Plugin"."kind") AS "root"))), \'[]\') AS "plugins" FROM (SELECT * FROM "Plugin" WHERE "Plugin"."appId" = "base"."id" AND "Plugin"."branch" = "base"."branch") AS "base.Plugin") AS "root.Plugin" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Schema"."id", "base.Schema"."branch", "base.Schema"."schema") AS "root"))), \'[]\') AS "schemas" FROM (SELECT * FROM "Schema" WHERE "Schema"."appId" = "base"."id" AND "Schema"."branch" = "base"."branch") AS "base.Schema") AS "root.Schema" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Style"."id", "base.Style"."branch", "base.Style"."name", "base.Style"."kind", "base.Style"."styles", "base.Style"."isDefault") AS "root"))), \'[]\') AS "styles" FROM (SELECT * FROM "Style" WHERE "Style"."appId" = "base"."id" AND "Style"."branch" = "base"."branch") AS "base.Style") AS "root.Style" ON (\'true\') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Workflow"."id", "base.Workflow"."branch", "base.Workflow"."name", "base.Workflow"."args", "steps") AS "root"))), \'[]\') AS "workflows" FROM (SELECT * FROM "Workflow" WHERE "Workflow"."appId" = "base"."id" AND "Workflow"."branch" = "base"."branch") AS "base.Workflow" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Workflow.Step"."id", "base.Workflow.Step"."branch", "base.Workflow.Step"."parentId", "base.Workflow.Step"."kind", "base.Workflow.Step"."kindId", "base.Workflow.Step"."data", "base.Workflow.Step"."order") AS "root"))), \'[]\') AS "steps" FROM (SELECT * FROM "Step" WHERE "Step"."workflowId" = "base.Workflow"."id" AND "Step"."branch" = "base.Workflow"."branch" ORDER BY "order" ASC) AS "base.Workflow.Step") AS "root.Step" ON (\'true\')) AS "root.Workflow" ON (\'true\'))) AS "data"`;

  const result = gql2Sql(fixture, {
    orgId: '123',
    appId: '456',
    branch: 'master',
  })
  t.is(result?.sql, expected)
  t.deepEqual(result.params, ['123', '456', 'master'])
})
