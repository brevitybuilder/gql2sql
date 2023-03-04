import test from 'ava'

import { gql2Sql } from '../index'
const gql = String.raw

test('sync function from native code', (t) => {
  const fixture = gql`
    query App {
      App(filter: { id: { eq: "345810043118026832" } }, order: { name: ASC }) {
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
      Component_aggregate(filter: { appId: { eq: "345810043118026832" } }) {
        count
        min {
          createdAt
        }
      }
    }
  `
  const buf = new TextEncoder().encode(fixture)
  const result = gql2Sql(buf)
  const query = new TextDecoder().decode(result)
  t.is(
    query,
    `SELECT json_build_object('App', (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base"."id", "components") AS "root"))), '[]') AS "root" FROM (SELECT * FROM "App" WHERE "id" = '345810043118026832' ORDER BY "name" ASC) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "pageMeta", "elements") AS "root"))), '[]') AS "components" FROM (SELECT * FROM "Component" WHERE "Component"."appId" = "base"."id") AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.PageMeta"."id", "base.Component.PageMeta"."path") AS "root")) AS "pageMeta" FROM (SELECT * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.Element"."id", "base.Component.Element"."name") AS "root"))), '[]') AS "elements" FROM (SELECT * FROM "Element" WHERE "Element"."componentParentId" = "base.Component"."id" ORDER BY "order" ASC) AS "base.Component.Element") AS "root.Element" ON ('true')) AS "root.Component" ON ('true')), 'Component_aggregate', (SELECT json_build_object('count', COUNT(*), 'min', json_build_object('createdAt', MIN("createdAt"))) AS "root" FROM (SELECT * FROM "Component" WHERE "appId" = '345810043118026832') AS "base")) AS "data"`,
  )
})

test('complex query', (t) => {
  const fixture = gql`
    query GetApp($orgId: String!, $appId: String!, $branch: String!) {
      app: App_one(filter: { orgId: { eq: $orgId }, id: { eq: $appId } }) {
        orgId
        id
        name
        description
        theme
        favicon
        customCSS
        analytics
        customDomain
        branches(filter: { appId: { eq: $appId } }) @relation(table: "Branch") {
          appId
          slug
          createdAt
        }
        components(
          filter: { appId: { eq: $appId }, or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Component", field: ["appId"], references: ["id"]) {
          id
          branch
          ... on PageMeta
            @relation(
              table: "PageMeta"
              field: ["componentId"]
              references: ["id"]
              single: true
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) {
            kind @static(value: "page")
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
              field: ["componentId"]
              references: ["id"]
              single: true
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) {
            kind @static(value: "customComponent")
            title
            sources(
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) @relation(table: "Source", field: ["componentId", "branch"], references: ["id", "branch"]) {
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
              elementId
              component(
                filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
                distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
              ) @relation(table: "Element", field: ["id"], references: ["elementId"], single: true) {
                id
                branch
                name
                kind
                source
                styles
                props
              }
              utility(
                filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
                distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
              ) @relation(table: "Utility", field: ["id"], references: ["utilityId"], single: true) {
                id
                branch
                name
                kind
                kindId
                data
              }
            }
            events(
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) @relation(table: "Event", field: ["componentMetaId"], references: ["id"]) {
              id
              branch
              name
              label
              help
              type
            }
          }
        }
        connections(
          filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Connection", field: ["appId"], references: ["id"]) {
          id
          branch
          name
          kind
          prodUrl
          mutationSchema(
            filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
            distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
          ) @relation(table: "Schema", field: ["id"], references: ["mutationSchemaId"], single: true) {
            id
            branch
            schema
          }
          querySchemaId
          endpoints(
            filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
            distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
          ) @relation(table: "Endpoint", field: ["connectionId"], references: ["id"]) {
            id
            branch
            name
            method
            path
            responseSchemaId
            responseIsList
            connectionId
            headers(
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) @relation(table: "Header", field: ["parentEndpointId"], references: ["id"]) {
              id
              branch
              key
              value
              dynamic
            }
            search(
              filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
              distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            ) @relation(table: "Search", field: ["endpointId"], references: ["id"]) {
              id
              branch
              key
              value
              dynamic
            }
          }
          headers(
            filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
            distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
          ) @relation(table: "Header", field: ["parentConnectionId"], references: ["id"]) {
            id
            branch
            key
            value
            dynamic
          }
        }
        plugins(
          filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Plugin", field: ["appId"], references: ["id"]) {
          id
          kind
        }
        schemas(
          filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Schema", field: ["appId"], references: ["id"]) {
          id
          branch
          schema
          connectionId
        }
        styles(
          filter: { or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Style", field: ["appId"], references: ["id"]) {
          id
          branch
          name
          kind
          styles
          isDefault
        }
        workflows(
          filter: { kind: { eq: "backend" }, or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
          distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
        ) @relation(table: "Workflow", field: ["appId"], references: ["id"]) {
          id
          branch
          name
          args
          type
          steps(
            filter: { kind: { eq: "backend" }, or: [{ branch: { eq: $branch } }, { branch: { eq: "main" } }] }
            distinct: { on: ["id"], order: [{ expr: { branch: { eq: $branch } }, dir: DESC }] }
            order: [{ orderKey: ASC }]
          ) @relation(table: "Step", field: ["workflowId"], references: ["id"]) {
            id
            branch
            parentId
            kind
            kindId
            data
            orderKey
          }
        }
      }
    }
  `
  const expected = `SELECT json_build_object('app', (SELECT to_json((SELECT "root" FROM (SELECT "base"."orgId", "base"."id", "base"."name", "base"."description", "base"."theme", "base"."favicon", "base"."customCSS", "base"."analytics", "base"."customDomain", "branches", "components", "connections", "plugins", "schemas", "styles", "workflows") AS "root")) AS "root" FROM (SELECT * FROM "App" WHERE "id" = $2 AND "orgId" = $1 LIMIT 1) AS "base" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Branch"."appId", "base.Branch"."slug", "base.Branch"."createdAt") AS "root"))), '[]') AS "branches" FROM (SELECT * FROM "Branch" WHERE "appId" = $2) AS "base.Branch") AS "root.Branch" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(CAST(to_json((SELECT "root" FROM (SELECT "base.Component"."id", "base.Component"."branch") AS "root")) AS jsonb) || CASE WHEN "root.PageMeta"."PageMeta" IS NOT NULL THEN to_jsonb("PageMeta") WHEN "root.ComponentMeta"."ComponentMeta" IS NOT NULL THEN to_jsonb("ComponentMeta") ELSE jsonb_build_object() END), '[]') AS "components" FROM (SELECT DISTINCT ON ("id") * FROM "Component" WHERE "Component"."appId" = "base"."id" AND "appId" = $2 AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Component" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT 'page' AS "kind", "base.Component.PageMeta"."title", "base.Component.PageMeta"."description", "base.Component.PageMeta"."path", "base.Component.PageMeta"."socialImage", "base.Component.PageMeta"."urlParams", "base.Component.PageMeta"."loader", "base.Component.PageMeta"."protection", "base.Component.PageMeta"."maxAge", "base.Component.PageMeta"."sMaxAge", "base.Component.PageMeta"."staleWhileRevalidate") AS "root")) AS "PageMeta" FROM (SELECT DISTINCT ON ("id") * FROM "PageMeta" WHERE "PageMeta"."componentId" = "base.Component"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC LIMIT 1) AS "base.Component.PageMeta") AS "root.PageMeta" ON ('true') LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT 'customComponent' AS "kind", "base.Component.ComponentMeta"."title", "sources", "events") AS "root")) AS "ComponentMeta" FROM (SELECT DISTINCT ON ("id") * FROM "ComponentMeta" WHERE "ComponentMeta"."componentId" = "base.Component"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC LIMIT 1) AS "base.Component.ComponentMeta" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source"."id", "base.Component.ComponentMeta.Source"."branch", "base.Component.ComponentMeta.Source"."name", "base.Component.ComponentMeta.Source"."provider", "base.Component.ComponentMeta.Source"."description", "base.Component.ComponentMeta.Source"."template", "base.Component.ComponentMeta.Source"."instanceTemplate", "base.Component.ComponentMeta.Source"."outputType", "base.Component.ComponentMeta.Source"."source", "base.Component.ComponentMeta.Source"."sourceProp", "base.Component.ComponentMeta.Source"."componentId", "base.Component.ComponentMeta.Source"."utilityId", "base.Component.ComponentMeta.Source"."elementId", "component", "utility") AS "root"))), '[]') AS "sources" FROM (SELECT DISTINCT ON ("id") * FROM "Source" WHERE "Source"."componentId" = "base.Component.ComponentMeta"."id" AND "Source"."branch" = "base.Component.ComponentMeta"."branch" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Component.ComponentMeta.Source" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source.Element"."id", "base.Component.ComponentMeta.Source.Element"."branch", "base.Component.ComponentMeta.Source.Element"."name", "base.Component.ComponentMeta.Source.Element"."kind", "base.Component.ComponentMeta.Source.Element"."source", "base.Component.ComponentMeta.Source.Element"."styles", "base.Component.ComponentMeta.Source.Element"."props") AS "root")) AS "component" FROM (SELECT DISTINCT ON ("id") * FROM "Element" WHERE "Element"."id" = "base.Component.ComponentMeta.Source"."elementId" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC LIMIT 1) AS "base.Component.ComponentMeta.Source.Element") AS "root.Element" ON ('true') LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Source.Utility"."id", "base.Component.ComponentMeta.Source.Utility"."branch", "base.Component.ComponentMeta.Source.Utility"."name", "base.Component.ComponentMeta.Source.Utility"."kind", "base.Component.ComponentMeta.Source.Utility"."kindId", "base.Component.ComponentMeta.Source.Utility"."data") AS "root")) AS "utility" FROM (SELECT DISTINCT ON ("id") * FROM "Utility" WHERE "Utility"."id" = "base.Component.ComponentMeta.Source"."utilityId" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC LIMIT 1) AS "base.Component.ComponentMeta.Source.Utility") AS "root.Utility" ON ('true')) AS "root.Source" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Component.ComponentMeta.Event"."id", "base.Component.ComponentMeta.Event"."branch", "base.Component.ComponentMeta.Event"."name", "base.Component.ComponentMeta.Event"."label", "base.Component.ComponentMeta.Event"."help", "base.Component.ComponentMeta.Event"."type") AS "root"))), '[]') AS "events" FROM (SELECT DISTINCT ON ("id") * FROM "Event" WHERE "Event"."componentMetaId" = "base.Component.ComponentMeta"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Component.ComponentMeta.Event") AS "root.Event" ON ('true')) AS "root.ComponentMeta" ON ('true')) AS "root.Component" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection"."id", "base.Connection"."branch", "base.Connection"."name", "base.Connection"."kind", "base.Connection"."prodUrl", "mutationSchema", "base.Connection"."querySchemaId", "endpoints", "headers") AS "root"))), '[]') AS "connections" FROM (SELECT DISTINCT ON ("id") * FROM "Connection" WHERE "Connection"."appId" = "base"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Connection" LEFT JOIN LATERAL (SELECT to_json((SELECT "root" FROM (SELECT "base.Connection.Schema"."id", "base.Connection.Schema"."branch", "base.Connection.Schema"."schema") AS "root")) AS "mutationSchema" FROM (SELECT DISTINCT ON ("id") * FROM "Schema" WHERE "Schema"."id" = "base.Connection"."mutationSchemaId" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC LIMIT 1) AS "base.Connection.Schema") AS "root.Schema" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint"."id", "base.Connection.Endpoint"."branch", "base.Connection.Endpoint"."name", "base.Connection.Endpoint"."method", "base.Connection.Endpoint"."path", "base.Connection.Endpoint"."responseSchemaId", "base.Connection.Endpoint"."responseIsList", "base.Connection.Endpoint"."connectionId", "headers", "search") AS "root"))), '[]') AS "endpoints" FROM (SELECT DISTINCT ON ("id") * FROM "Endpoint" WHERE "Endpoint"."connectionId" = "base.Connection"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Connection.Endpoint" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint.Header"."id", "base.Connection.Endpoint.Header"."branch", "base.Connection.Endpoint.Header"."key", "base.Connection.Endpoint.Header"."value", "base.Connection.Endpoint.Header"."dynamic") AS "root"))), '[]') AS "headers" FROM (SELECT DISTINCT ON ("id") * FROM "Header" WHERE "Header"."parentEndpointId" = "base.Connection.Endpoint"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Connection.Endpoint.Header") AS "root.Header" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Endpoint.Search"."id", "base.Connection.Endpoint.Search"."branch", "base.Connection.Endpoint.Search"."key", "base.Connection.Endpoint.Search"."value", "base.Connection.Endpoint.Search"."dynamic") AS "root"))), '[]') AS "search" FROM (SELECT DISTINCT ON ("id") * FROM "Search" WHERE "Search"."endpointId" = "base.Connection.Endpoint"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Connection.Endpoint.Search") AS "root.Search" ON ('true')) AS "root.Endpoint" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Connection.Header"."id", "base.Connection.Header"."branch", "base.Connection.Header"."key", "base.Connection.Header"."value", "base.Connection.Header"."dynamic") AS "root"))), '[]') AS "headers" FROM (SELECT DISTINCT ON ("id") * FROM "Header" WHERE "Header"."parentConnectionId" = "base.Connection"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Connection.Header") AS "root.Header" ON ('true')) AS "root.Connection" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Plugin"."id", "base.Plugin"."kind") AS "root"))), '[]') AS "plugins" FROM (SELECT DISTINCT ON ("id") * FROM "Plugin" WHERE "Plugin"."appId" = "base"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Plugin") AS "root.Plugin" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Schema"."id", "base.Schema"."branch", "base.Schema"."schema", "base.Schema"."connectionId") AS "root"))), '[]') AS "schemas" FROM (SELECT DISTINCT ON ("id") * FROM "Schema" WHERE "Schema"."appId" = "base"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Schema") AS "root.Schema" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Style"."id", "base.Style"."branch", "base.Style"."name", "base.Style"."kind", "base.Style"."styles", "base.Style"."isDefault") AS "root"))), '[]') AS "styles" FROM (SELECT DISTINCT ON ("id") * FROM "Style" WHERE "Style"."appId" = "base"."id" AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Style") AS "root.Style" ON ('true') LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Workflow"."id", "base.Workflow"."branch", "base.Workflow"."name", "base.Workflow"."args", "base.Workflow"."type", "steps") AS "root"))), '[]') AS "workflows" FROM (SELECT DISTINCT ON ("id") * FROM "Workflow" WHERE "Workflow"."appId" = "base"."id" AND "kind" = 'backend' AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS "base.Workflow" LEFT JOIN LATERAL (SELECT coalesce(json_agg(to_json((SELECT "root" FROM (SELECT "base.Workflow.Step"."id", "base.Workflow.Step"."branch", "base.Workflow.Step"."parentId", "base.Workflow.Step"."kind", "base.Workflow.Step"."kindId", "base.Workflow.Step"."data", "base.Workflow.Step"."orderKey") AS "root"))), '[]') AS "steps" FROM (SELECT * FROM (SELECT DISTINCT ON ("id") * FROM "Step" WHERE "Step"."workflowId" = "base.Workflow"."id" AND "kind" = 'backend' AND ("branch" = $3 OR "branch" = 'main') ORDER BY "id" ASC, "branch" = $3 DESC) AS sorter ORDER BY "orderKey" ASC) AS "base.Workflow.Step") AS "root.Step" ON ('true')) AS "root.Workflow" ON ('true'))) AS "data"`

  const buf = Buffer.from(fixture);
  const result = gql2Sql(buf);
  const query = result.toString('utf8');
  t.is(query, expected)
})
