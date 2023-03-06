from flask import Flask, render_template

app = Flask(__name__)

authors = [
    {
        "id": 100,
        "name": "John",
        "likes": [
            202,
            200
        ]
    },
    {
        "id": 101,
        "name": "jane",
        "likes": [
            200
        ]
    }
]

posts = [
    {
        "id": 200,
        "author": 100,
        "content": "Difficulty on insensible reasonable in. From as went he they."
    },
    {
        "id": 201,
        "author": 100,
        "content": "Preference themselves me as thoroughly partiality considered on in estimating."
    },
    {
        "id": 202,
        "author": 101,
        "content": "In as name to here them deny wise this. As rapid woody my he me which."
    }
]


# Making list from two dictionaries
def transformed_posts(authors, posts):
    transformed_posts_list = []
    likes = []
    post_dict = {}
    for p in posts:
        for a in authors:
            post_dict = {'id': p['id'], 'author': a['name'], 'content': p['content']}

            if p['author'] == a['id']:
                p['author'] = a['name']

            for like in a['likes']:
                if like == p['id']:
                    likes.append(a['name'])
            post_dict['likes'] = likes
        transformed_posts_list.append(post_dict)
    print(transformed_posts_list)
    return transformed_posts_list


transformed_posts = transformed_posts(authors, posts)


@app.route("/posts")
def list_posts():
    return render_template("index.html", posts=transformed_posts)


if __name__ == '__main__':
    app.run()
