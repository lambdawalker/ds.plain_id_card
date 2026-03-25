import json
import os

for f in os.listdir("./output/obj"):
    f = os.path.join("./output/obj", f).replace("\\", "/")
    with open(f, "r") as file:
        try:
            json.loads(file.read())
        except:
            print(f)
