"""Run the Finance web app locally."""

from finance.src.web.app import app

if __name__ == "__main__":
    app.run(debug=True, port=5001)
