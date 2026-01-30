from lambdawaker.templete.server.TemplateServer import TemplateServer

server = TemplateServer(
    "./templates",
    ["@DS/idPhotoV1.0.0"]
)

app = server.app
