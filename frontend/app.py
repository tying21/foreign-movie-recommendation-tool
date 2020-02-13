from flask import Flask, render_template

# initialize flask
app = Flask(__name__)


@app.route('/')
def main():
    return render_template("tab.html")


if __name__ == "__main__":    
    # point from domain name to ec2
    app.run(host="0.0.0.0", port=80)
