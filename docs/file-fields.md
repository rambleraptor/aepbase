# File Uploads

```notice
The AEP spec does not currently have support for File fields
on resources or multipart requests.

Support is **off by default**. Please use aepbase as a library
to enable support for file fields.
```

aepbase has experimental support for **file fields** — properties on a
resource whose value is a binary file stored on disk. Clients upload file
contents with the create / update request and see a download URL in
subsequent reads.

## Enabling file fields

File-field support is a library-only opt-in. Enable it via `ServerOptions`:

```go
err := aepbase.Run(aepbase.ServerOptions{
    Port:             8080,
    DataDir:          "aepbase_data",
    EnableFileFields: true, // files stored under aepbase_data/files/...
})
```

## Declaring a file field

Mark the property with both `type: binary` and `x-aepbase-file-field: true`:

```json
{
  "singular": "document",
  "plural": "documents",
  "schema": {
    "properties": {
      "title": {"type": "string"},
      "body": {
        "type": "binary",
        "x-aepbase-file-field": true
      }
    },
    "required": ["title", "body"]
  }
}
```

Required file fields must be uploaded on create — an empty multipart will
return 400.

## Uploading files

Create and update requests accept `multipart/form-data`:

```sh
curl -X POST http://localhost:8080/documents?id=doc1 \
  -F 'resource={"title":"my doc"};type=application/json' \
  -F 'body=@./report.pdf'
```

Plain `application/json` bodies are still accepted for resources with file
fields — you just cannot set a file field's value that way. JSON values on
file fields are rejected with 400.

## Reading files

### GET / LIST responses

When the file exists on disk, the field is rendered as an absolute URL
pointing at the auto-registered `:download` custom method:

```json
{
  "id": "doc1",
  "path": "documents/doc1",
  "title": "my doc",
  "body": "http://localhost:8080/documents/doc1:download?field=body"
}
```

When the file is absent the field is **omitted entirely** from the response
(not `null`).

### Downloading bytes

POST to the `:download` custom method with a JSON body naming the field:

```sh
curl -X POST http://localhost:8080/documents/doc1:download \
  -H 'Content-Type: application/json' \
  -d '{"field":"body"}' \
  --output report.pdf
```

The response is `application/octet-stream` with a `Content-Disposition`
header. Downloads for missing files return 404; downloads for unknown
field names return 400.

## Storage layout on disk

Files live under `{filesDir}` in a hierarchy mirroring the resource path:

```
filesDir/
  documents/
    doc1/
      body
  publishers/
    acme/
      books/
        book1/
          cover
```

`filesDir` defaults to `{DataDir}/files` when you use `ServerOptions`; when
you call `state.EnableFileFields(dir)` directly you pass the directory.
