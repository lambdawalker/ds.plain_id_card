from lambdawalker.template.server.TemplateServer import TemplateServer

server = TemplateServer(
    "./templates",
    [
        "@DS/ds.photo_id",
        "@DS/ds.machine_readable_code"
    ]
)

app = server.app
