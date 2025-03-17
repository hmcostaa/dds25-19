from quart import Quart

app = Quart(__name__)


@app.route("/")
async def index() -> str:
    return {"key": "value"}

if __name__ == "__main__":
    app.run(debug=True)
