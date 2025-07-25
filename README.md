from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import sqlite3

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///baggage.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class BagScan(db.Model):
    __tablename__ = 'bag_scans'
    
    scan_internal_id = db.Column(db.Integer, primary_key=True)
    bag_tag_id = db.Column(db.String(50), nullable=False)
    destination_gate = db.Column(db.String(10), nullable=False)
    location_scanned = db.Column(db.String(50), nullable=False)
    scan_timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'scan_internal_id': self.scan_internal_id,
            'bag_tag_id': self.bag_tag_id,
            'destination_gate': self.destination_gate,
            'location_scanned': self.location_scanned,
            'scan_timestamp': self.scan_timestamp.isoformat()
        }

with app.app_context():
    db.create_all()


@app.route('/baggage/scan', methods=['POST'])
def log_scan():
    """Log a new baggage scan"""
    data = request.get_json()
    
    # Validate required fields
    required_fields = ['bag_tag_id', 'destination_gate', 'location_scanned']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing required fields'}), 400
    
    # Create new scan record
    new_scan = BagScan(
        bag_tag_id=data['bag_tag_id'],
        destination_gate=data['destination_gate'],
        location_scanned=data['location_scanned']
    )
    
    db.session.add(new_scan)
    db.session.commit()
    
    return jsonify({
        'scan_internal_id': new_scan.scan_internal_id,
        'status': 'logged'
    }), 201

@app.route('/baggage/scans/bag/<bag_tag_id>', methods=['GET'])
def get_bag_scans(bag_tag_id):
    """Get all scans for a specific bag"""
    scans = BagScan.query.filter_by(bag_tag_id=bag_tag_id)\
                       .order_by(BagScan.scan_timestamp.desc())\
                       .all()
    
    # Handle latest=true parameter
    if request.args.get('latest', '').lower() == 'true':
        if not scans:
            return jsonify({'error': 'No scans found'}), 404
        return jsonify(scans[0].to_dict())
    
    return jsonify([scan.to_dict() for scan in scans])

@app.route('/baggage/scans/gate/<destination_gate>', methods=['GET'])
def get_gate_scans(destination_gate):
    """Get all scans for a specific gate"""
    scans = BagScan.query.filter_by(destination_gate=destination_gate)\
                       .order_by(BagScan.scan_timestamp.desc())\
                       .all()
    
    return jsonify([scan.to_dict() for scan in scans])

## Milestone Endpoints ##

@app.route('/baggage/active/gate/<destination_gate>', methods=['GET'])
def get_active_bags(destination_gate):
    """Get unique active bags for a gate"""
    try:
        since_minutes = int(request.args.get('sinceminutes', 60))
    except ValueError:
        return jsonify({'error': 'sinceminutes must be an integer'}), 400
    
    time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)
    
    # Get unique bags with their latest scan info
    subquery = db.session.query(
        BagScan.bag_tag_id,
        db.func.max(BagScan.scan_timestamp).label('latest_scan')
    ).filter(
        BagScan.destination_gate == destination_gate,
        BagScan.scan_timestamp >= time_threshold
    ).group_by(BagScan.bag_tag_id).subquery()
    
    results = db.session.query(BagScan).join(
        subquery,
        db.and_(
            BagScan.bag_tag_id == subquery.c.bag_tag_id,
            BagScan.scan_timestamp == subquery.c.latest_scan
        )
    ).all()
    
    return jsonify([{
        'bag_tag_id': r.bag_tag_id,
        'last_scan_at': r.scan_timestamp.isoformat(),
        'last_location': r.location_scanned
    } for r in results])

@app.route('/baggage/stats/gate-counts', methods=['GET'])
def get_gate_counts():
    """Get bag counts per gate"""
    try:
        since_minutes = int(request.args.get('sinceminutes', 60))
    except ValueError:
        return jsonify({'error': 'sinceminutes must be an integer'}), 400
    
    time_threshold = datetime.utcnow() - timedelta(minutes=since_minutes)
    
    results = db.session.query(
        BagScan.destination_gate,
        db.func.count(db.distinct(BagScan.bag_tag_id)).label('count')
    ).filter(
        BagScan.scan_timestamp >= time_threshold
    ).group_by(BagScan.destination_gate).all()
    
    return jsonify([{
        'destination_gate': gate,
        'unique_bag_count': count
    } for gate, count in results])

if __name__ == '__main__':
    app.run(debug=True)
