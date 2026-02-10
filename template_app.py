from lambdawaker.template.server.TemplateServer import TemplateServer

server = TemplateServer(
    "./templates",
    [
        "@DS/ds.idPhoto",
        "@DS/ds.machineReadableCode"
    ]
)

app = server.app
