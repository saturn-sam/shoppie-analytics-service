
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
# from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity
from flask_migrate import Migrate
import jwt
import os
import pika
import json
import datetime
import threading
import time
from werkzeug.exceptions import BadRequest, Unauthorized, NotFound

app = Flask(__name__)
CORS(app)

# Configuration
# app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@analytics-db:5432/analytics_db')
uri = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/analytics_db')
if uri.startswith('postgres://'):
    uri = uri.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = uri

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'your_jwt_secret_key')

# Initialize extensions
db = SQLAlchemy(app)
migrate = Migrate(app, db)
# jwt = JWTManager(app)

import logging
import sys
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
handler.setLevel(logging.INFO)

app.logger.handlers = [handler]
app.logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('/var/log/analytics.log')
file_handler.setFormatter(JsonFormatter())
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)


# RabbitMQ connection
def get_rabbitmq_connection():
    rabbitmq_url = os.environ.get('MESSAGE_QUEUE_URL', 'amqp://guest:guest@rabbitmq:5672')
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    return connection

# Models
class Sale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    order_id = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.String(50), nullable=False)
    total_amount = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    items_count = db.Column(db.Integer, nullable=False)

class ProductSale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, nullable=False)
    product_name = db.Column(db.String(200), nullable=False)
    price = db.Column(db.Float, nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    order_id = db.Column(db.Integer, nullable=False)
    date = db.Column(db.DateTime, default=datetime.datetime.utcnow)

class UserActivity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(50), nullable=False)
    action = db.Column(db.String(100), nullable=False)
    data = db.Column(db.JSON)
    timestamp = db.Column(db.DateTime, default=datetime.datetime.utcnow)

class UserMetrics(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    new_users = db.Column(db.Integer, default=0)
    active_users = db.Column(db.Integer, default=0)
    orders_placed = db.Column(db.Integer, default=0)
    total_revenue = db.Column(db.Float, default=0.0)

# Create tables
# with app.app_context():
#     db.create_all()


JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key')  # Same as used to encode the token
ALGORITHM = 'HS256'             # Or the algorithm you used to sign the token

def get_user_from_token():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None

    token = auth_header.split(' ')[1]

    try:
        # Decode the JWT
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get('user_id')
        if not user_id:
            return None
        
        # (Optional) Fetch user from DB
        # user = UserModel.query.get(user_id)
        # return user

        return {'id': user_id}

    except jwt.ExpiredSignatureError:
        app.logger.warning("Token has expired")
        return None
    except jwt.InvalidTokenError:
        app.logger.warning("Invalid token")
        return None
    

def token_required(f):
    def decorator(*args, **kwargs):
        user = get_user_from_token()
        if not user:
            raise Unauthorized('Authentication required')
        request.user = user
        return f(*args, **kwargs)
    decorator.__name__ = f.__name__
    return decorator

# Health check endpoint
@app.route('/analytics-api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy', 'service': 'analytics-service'}), 200

# Endpoints
@app.route('/analytics-api/analytics/sales', methods=['GET'])
# @jwt_required()
@token_required
def get_sales_data():
    user_id = get_user_from_token()['id']
    
    # Check if user is staff (this would typically be handled by role-based authorization)
    # For this example, assume all authenticated users can access their own sales data
    
    period = request.args.get('period', 'monthly')
    
    # Get date range based on period
    today = datetime.datetime.utcnow().date()
    
    if period == 'daily':
        start_date = today - datetime.timedelta(days=30)
        date_format = '%Y-%m-%d'
        query = db.session.query(
            db.func.date(Sale.date).label('day'),
            db.func.count(Sale.id).label('orders'),
            db.func.sum(Sale.total_amount).label('sales')
        ).filter(Sale.date >= start_date).group_by('day').order_by('day')
    
    elif period == 'weekly':
        start_date = today - datetime.timedelta(weeks=12)
        query = db.session.query(
            db.func.date_trunc('week', Sale.date).label('week'),
            db.func.count(Sale.id).label('orders'),
            db.func.sum(Sale.total_amount).label('sales')
        ).filter(Sale.date >= start_date).group_by('week').order_by('week')
        date_format = '%Y-%m-%d'  # Week start date
    
    elif period == 'yearly':
        query = db.session.query(
            db.func.date_trunc('year', Sale.date).label('year'),
            db.func.count(Sale.id).label('orders'),
            db.func.sum(Sale.total_amount).label('sales')
        ).group_by('year').order_by('year')
        date_format = '%Y'
    
    else:  # monthly (default)
        start_date = today.replace(day=1, month=today.month-11) if today.month > 11 else today.replace(day=1, month=today.month+1, year=today.year-1)
        query = db.session.query(
            db.func.date_trunc('month', Sale.date).label('month'),
            db.func.count(Sale.id).label('orders'),
            db.func.sum(Sale.total_amount).label('sales')
        ).filter(Sale.date >= start_date).group_by('month').order_by('month')
        date_format = '%Y-%m'
    
    results = query.all()
    
    # Calculate totals and growth
    data = []
    total_sales = 0
    total_orders = 0
    
    for row in results:
        period_date = row[0].strftime(date_format) if row[0] else 'N/A'
        orders = row[1] or 0
        sales = float(row[2] or 0)
        
        data.append({
            'date': period_date,
            'orders': orders,
            'sales': sales
        })
        
        total_sales += sales
        total_orders += orders
    
    # Calculate growth (compare with previous period)
    growth = 0
    if len(data) >= 2:
        current_period = data[-1]['sales']
        previous_period = data[-2]['sales']
        if previous_period > 0:
            growth = ((current_period - previous_period) / previous_period) * 100
    
    return jsonify({
        'period': period,
        'data': data,
        'totalSales': total_sales,
        'totalOrders': total_orders,
        'growth': round(growth, 2)
    })

@app.route('/analytics-api/analytics/products', methods=['GET'])
@token_required
def get_product_analytics():
    # This would typically be restricted to staff users
    
    # Get product performance metrics
    query = db.session.query(
        ProductSale.product_id,
        ProductSale.product_name,
        db.func.sum(ProductSale.quantity * ProductSale.price).label('total_revenue'),
        db.func.sum(ProductSale.quantity).label('total_sales'),
        db.func.avg(ProductSale.price).label('average_price')
    ).group_by(ProductSale.product_id, ProductSale.product_name).order_by(db.text('total_revenue DESC'))
    
    results = query.limit(20).all()  # Top 20 products
    
    product_analytics = []
    for row in results:
        product_analytics.append({
            'productId': row[0],
            'productName': row[1],
            'totalRevenue': float(row[2] or 0),
            'totalSales': int(row[3] or 0),
            'conversionRate': 2.5,  # Placeholder - would be calculated from view/sale ratio
            'averageRating': 4.2  # Placeholder - would be calculated from product reviews
        })
    
    return jsonify(product_analytics)

@app.route('/analytics-api/analytics/products/<int:product_id>', methods=['GET'])
@token_required
def get_product_detail_analytics(product_id):
    # This would typically be restricted to staff users
    
    # Get detailed metrics for a specific product
    product_data = db.session.query(
        ProductSale.product_name,
        db.func.sum(ProductSale.quantity * ProductSale.price).label('total_revenue'),
        db.func.sum(ProductSale.quantity).label('total_sales'),
        db.func.avg(ProductSale.price).label('average_price')
    ).filter(ProductSale.product_id == product_id).group_by(ProductSale.product_name).first()
    
    if not product_data:
        return jsonify([]), 404
    
    # Get sales trend data for this product
    thirty_days_ago = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    
    trend_data = db.session.query(
        db.func.date(ProductSale.date).label('day'),
        db.func.sum(ProductSale.quantity).label('sales')
    ).filter(
        ProductSale.product_id == product_id,
        ProductSale.date >= thirty_days_ago
    ).group_by('day').order_by('day').all()
    
    trend = [{
        'date': row[0].strftime('%Y-%m-%d'),
        'sales': row[1] or 0
    } for row in trend_data]
    
    # Single product analytics (placeholder values for demo)
    return jsonify([{
        'productId': product_id,
        'productName': product_data[0],
        'totalRevenue': float(product_data[1] or 0),
        'totalSales': int(product_data[2] or 0),
        'conversionRate': 2.8,  # Placeholder
        'averageRating': 4.3,  # Placeholder
        'salesTrend': trend
    }])

@app.route('/analytics-api/analytics/users', methods=['GET'])
@token_required
def get_user_analytics():
    # This would typically be restricted to staff users
    
    # Get latest user metrics
    latest_metrics = UserMetrics.query.order_by(UserMetrics.date.desc()).first()
    
    # If no metrics exist, return placeholders
    if not latest_metrics:
        return jsonify({
            'newUsers': 0,
            'activeUsers': 0,
            'conversionRate': 0,
            'averageOrderValue': 0,
            'retentionRate': 0
        })
    
    # Calculate average order value
    avg_order = db.session.query(db.func.avg(Sale.total_amount)).scalar() or 0
    
    # Return user metrics
    return jsonify({
        'newUsers': latest_metrics.new_users,
        'activeUsers': latest_metrics.active_users,
        'conversionRate': latest_metrics.orders_placed / latest_metrics.active_users if latest_metrics.active_users > 0 else 0,
        'averageOrderValue': float(avg_order),
        'retentionRate': 65.5  # Placeholder value
    })

@app.route('/analytics-api/analytics/dashboard', methods=['GET'])
@token_required
def get_dashboard_metrics():
    # This would typically be restricted to staff users
    
    # Get overall metrics
    total_revenue = db.session.query(db.func.sum(Sale.total_amount)).scalar() or 0
    total_orders = db.session.query(db.func.count(Sale.id)).scalar() or 0
    total_customers = db.session.query(db.func.count(db.distinct(Sale.user_id))).scalar() or 0
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    # Get top products
    top_products_query = db.session.query(
        ProductSale.product_id,
        ProductSale.product_name,
        db.func.sum(ProductSale.quantity * ProductSale.price).label('revenue'),
        db.func.sum(ProductSale.quantity).label('quantity')
    ).group_by(ProductSale.product_id, ProductSale.product_name).order_by(db.text('revenue DESC')).limit(5)
    
    top_products = [{
        'id': row[0],
        'name': row[1],
        'revenue': float(row[2] or 0),
        'quantity': int(row[3] or 0)
    } for row in top_products_query.all()]
    
    # Get recent orders
    recent_orders_query = Sale.query.order_by(Sale.date.desc()).limit(5)
    
    recent_orders = [{
        'id': sale.order_id,
        'date': sale.date.isoformat(),
        'customer': f"User {sale.user_id}",  # In a real app, you'd get the customer name
        'amount': sale.total_amount,
        'status': 'completed'  # Placeholder, would be fetched from order service
    } for sale in recent_orders_query.all()]
    
    # Create some placeholder data for sales by category
    # In a real implementation, this would be calculated from actual product categories
    sales_by_category = [
        {'category': 'Electronics', 'value': 12500},
        {'category': 'Clothing', 'value': 9800},
        {'category': 'Home & Kitchen', 'value': 7300},
        {'category': 'Books', 'value': 4500},
        {'category': 'Other', 'value': 3200}
    ]
    
    return jsonify({
        'totalRevenue': float(total_revenue),
        'totalOrders': total_orders,
        'totalCustomers': total_customers,
        'averageOrderValue': float(avg_order_value),
        'topProducts': top_products,
        'recentOrders': recent_orders,
        'salesByCategory': sales_by_category
    })

@app.route('/analytics-api/analytics/activity', methods=['POST'])
@token_required
def log_activity():
    user_id = get_user_from_token()['id']
    data = request.json
    
    # Log user activity
    activity = UserActivity(
        user_id=user_id,
        action=data.get('action', 'unknown'),
        data=data.get('data', {})
    )
    
    db.session.add(activity)
    db.session.commit()
    
    return jsonify({'success': True})

# Message queue consumer for processing events
def consume_events():
    try:
        # Connect to RabbitMQ
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declare exchanges for different event types
        channel.exchange_declare(exchange='order_events', exchange_type='topic', durable=True)
        channel.exchange_declare(exchange='payment_events', exchange_type='topic', durable=True)
        channel.exchange_declare(exchange='product_events', exchange_type='topic', durable=True)
        
        # Create and bind queue for analytics
        result = channel.queue_declare(queue='analytics_queue', durable=True)
        queue_name = result.method.queue
        
        # Bind to order events
        channel.queue_bind(exchange='order_events', queue=queue_name, routing_key='order.#')
        
        # Bind to payment events
        channel.queue_bind(exchange='payment_events', queue=queue_name, routing_key='payment.#')
        
        # Bind to product events
        channel.queue_bind(exchange='product_events', queue=queue_name, routing_key='product.#')
        
        def callback(ch, method, properties, body):
            try:
                # Process event
                event_data = json.loads(body)
                event_type = method.routing_key
                
                app.logger.info(f"Received event: {event_type}")
                
                # Handle different event types
                if event_type == 'order.created':
                    process_order_created(event_data)
                elif event_type == 'payment.completed':
                    process_payment_completed(event_data)
                
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                app.logger.error(f"Error processing message: {str(e)}")
                # Negative acknowledge
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set up consumer with manual acknowledgment
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        
        # Start consuming
        app.logger.info('Analytics service started consuming events')
        channel.start_consuming()
        
    except Exception as e:
        app.logger.error(f"RabbitMQ consumer error: {str(e)}")
        # Retry after delay
        time.sleep(5)
        consume_events()

def process_order_created(event_data):
    # Get order data from event
    order_data = event_data.get('data', {})
    
    order_id = order_data.get('orderId')
    user_id = order_data.get('userId')
    total_amount = order_data.get('totalAmount', 0)
    items = order_data.get('items', [])
    
    with app.app_context():
        # Create sale record
        sale = Sale(
            order_id=order_id,
            user_id=user_id,
            total_amount=total_amount,
            items_count=len(items)
        )
        db.session.add(sale)
        
        # Create product sale records for each item
        for item in items:
            product_sale = ProductSale(
                product_id=item.get('productId'),
                product_name=item.get('name', f"Product {item.get('productId')}"),
                price=item.get('price', 0),
                quantity=item.get('quantity', 1),
                order_id=order_id
            )
            db.session.add(product_sale)
        
        # Update user metrics for today
        today = datetime.datetime.utcnow().date()
        metrics = UserMetrics.query.filter_by(date=today).first()
        
        if not metrics:
            metrics = UserMetrics(date=today, orders_placed=0, total_revenue=0.0)
            db.session.add(metrics)
        
        metrics.orders_placed += 1
        metrics.total_revenue += total_amount
        
        db.session.commit()

def process_payment_completed(event_data):
    # In a real implementation, you might want to update payment status in sales data
    # or track payment methods used for analytics
    pass

# Start consumer thread
threading.Thread(target=consume_events, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
