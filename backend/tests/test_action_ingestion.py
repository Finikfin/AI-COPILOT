import unittest

from app.services.openapi_ingestion import extract_actions_from_document, load_openapi_document


SAMPLE_OPENAPI = b"""
openapi: 3.0.0
info:
  title: Minimal Marketing API
  version: 1.0.0
servers:
  - url: https://marketing.example.com/api
paths:
  /users:
    get:
      operationId: listUsers
      summary: List users
      tags: [users]
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/User"
  /users/{userId}:
    parameters:
      - in: path
        name: userId
        required: true
        schema:
          type: string
    get:
      operationId: getUser
      summary: Get user
      tags: [users]
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/User"
    patch:
      operationId: updateUser
      summary: Update user
      tags: [users]
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/UserUpdate"
      responses:
        "200":
          description: updated
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/User"
  /campaigns:
    post:
      operationId: createCampaign
      summary: Create campaign
      tags: [campaigns]
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [name]
              properties:
                name:
                  type: string
                segmentId:
                  type: string
      responses:
        "201":
          description: created
          content:
            application/json:
              schema:
                type: object
                properties:
                  id:
                    type: string
  /campaigns/{campaignId}/send:
    post:
      operationId: sendCampaign
      summary: Send campaign
      tags: [campaigns]
      parameters:
        - in: path
          name: campaignId
          required: true
          schema:
            type: string
      responses:
        "202":
          description: accepted
components:
  schemas:
    User:
      type: object
      properties:
        id:
          type: string
        email:
          type: string
    UserUpdate:
      type: object
      properties:
        email:
          type: string
"""


class OpenApiIngestionTests(unittest.TestCase):
    def test_extract_actions_from_document_returns_all_five_operations(self):
        document = load_openapi_document(SAMPLE_OPENAPI)

        actions = extract_actions_from_document(document, source_filename="marketing.yaml")

        self.assertEqual(len(actions), 5)
        self.assertEqual({action["operation_id"] for action in actions}, {
            "listUsers",
            "getUser",
            "updateUser",
            "createCampaign",
            "sendCampaign",
        })
        self.assertTrue(all(action["source_filename"] == "marketing.yaml" for action in actions))

        update_user = next(action for action in actions if action["operation_id"] == "updateUser")
        self.assertEqual(update_user["method"].value, "PATCH")
        self.assertEqual(update_user["parameters_schema"]["required"], ["userId"])
        self.assertEqual(update_user["request_body_schema"]["type"], "object")

        create_campaign = next(action for action in actions if action["operation_id"] == "createCampaign")
        self.assertEqual(create_campaign["request_body_schema"]["x-content-type"], "application/json")
        self.assertEqual(create_campaign["response_schema"]["x-content-type"], "application/json")

    def test_load_openapi_document_rejects_swagger_2(self):
        with self.assertRaisesRegex(ValueError, "Only OpenAPI 3.x documents are supported"):
            load_openapi_document(
                b"""
swagger: "2.0"
info:
  title: Legacy API
  version: "1.0"
paths:
  /ping:
    get:
      responses:
        "200":
          description: ok
"""
            )


if __name__ == "__main__":
    unittest.main()
