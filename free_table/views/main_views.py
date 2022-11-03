from flask import Blueprint, request, render_template, redirect, url_for
import sqlite3

bp = Blueprint('main', __name__, url_prefix='/')


@bp.route('/')
def homepage():
      return render_template('homepage.html')

@bp.route('/information')
def information():
      return render_template('information.html')

@bp.route('/product_search', methods=['POST','GET'])
def product_search():
      if request.method == 'POST':
            return redirect(url_for('product_search_result(product_name)'))
      return render_template('product_search.html')

@bp.route('/product_search_result', methods=['POST','GET'])
def product_search_result():
      check_box = request.args.get('allergies')
      
      if check_box == 'allergies-wheat': allergie = "'%"+'밀'+"%'"
      elif check_box == 'allergies-bean': allergie = "'%"+'콩'+"%'"
      elif check_box == 'allergies-pork': allergie = "'%"+'돼지'+"%'"
      elif check_box == 'allergies-chicken': allergie = "'%"+'닭'+"%'"
      elif check_box == 'allergies-peach': allergie = "'%"+'복숭아'+"%'"
      elif check_box == 'allergies-tomato': allergie = "'%"+'토마토'+"%'"
      else : allergie = 'None'
      
      product_name = "'%"+request.args.get('product_name')+"%'"

      conn = sqlite3.connect('search.db')
      cur = conn.cursor()
      
      SEL = f"SELECT SITE_NAME, IMAGE_ADDR, DB_ITEM_NAME, LPRICE, LINK_ADDR FROM get_site_list WHERE DB_ITEM_NAME LIKE {product_name} AND DB_ITEM_ING NOT LIKE {allergie}"
      
      cur.execute(SEL)
      product_list = cur.fetchall()
      
      return render_template('product_search_result.html', product_list=product_list)

@bp.route('/company_dashboard', methods=['POST','GET'])
def company_dashboard():
      # You'll need to install PyJWT via pip 'pip install PyJWT' or your project packages file

      import jwt
      import time

      METABASE_SITE_URL = "http://127.0.0.1:3000"
      METABASE_SECRET_KEY = "78f659d32ca22a9dffe42f67745cccffd382ff6f6fc7e9b4274f7ca3cf5e911b"

      payload = {
      "resource": {"dashboard": 1},
      "params": {
      
      },
      "exp": round(time.time()) + (60 * 10) # 10 minute expiration
      }
      token = jwt.encode(payload, METABASE_SECRET_KEY, algorithm="HS256")

      iframeUrl = METABASE_SITE_URL + "/embed/dashboard/" + token + "#bordered=true&titled=true"
      
      return render_template('company_dashboard.html', iframeUrl=iframeUrl)