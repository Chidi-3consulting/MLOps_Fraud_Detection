import json
import logging
import os
import random
import time
import signal
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from random import randint, choice

try:
    from confluent_kafka import Producer
except Exception:
    # If confluent_kafka is not installed (common in test/dev), provide a dummy
    # Producer implementation so the generator can run and still write to local sinks or DB.
    class DummyProducer:
        def __init__(self, *args, **kwargs):
            pass

        def produce(self, topic, key=None, value=None, callback=None):
            # simulate immediate success by invoking callback with None
            if callback:
                try:
                    callback(None, type('Msg', (), {'topic': lambda: topic, 'partition': lambda: 0})())
                except Exception:
                    pass

        def poll(self, timeout=0):
            return None

        def flush(self, timeout=None):
            return 0

        def close(self):
            return None

    Producer = DummyProducer
from dotenv import load_dotenv
from faker import Faker
from jsonschema import validate, ValidationError, FormatChecker
# Robust import: when running inside the producer container the files are copied
# into /app (same directory) so `producer.file_sink` may not be a package.


# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables from repo-root .env if present (safer than relative path)
try:
    _repo_env = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
    if os.path.exists(_repo_env):
        load_dotenv(_repo_env)
    else:
        # fall back to default location resolution
        load_dotenv(dotenv_path="../.env")
except Exception:
    load_dotenv(dotenv_path="../.env")

# Faker instance for generating random data
# Faker instance for generating Nigerian-focused data
fake = Faker('en_NG')  # Nigerian locale

# JSON Schema for e-commerce transaction validation
ECOMMERCE_TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        # Customer/Profile elements
        "user_id": {"type": "string"},
        "full_name": {"type": "string"},
        "email": {"type": "string", "format": "email"},
        "phone_number": {"type": "string"},
        "gender": {"type": "string", "enum": ["Male", "Female", "Other"]},
        "age": {"type": "integer", "minimum": 18, "maximum": 80},
        "registration_date": {"type": "string", "format": "date-time"},
        "account_status": {"type": "string", "enum": ["active", "suspended", "inactive"]},
        "device_id": {"type": "string"},
        "ip_address": {"type": "string", "format": "ipv4"},
        "location": {"type": "string"},
        "country": {"type": "string"},

        # Session/Behavioral elements
        "session_id": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "action": {"type": "string", "enum": ["login", "browse", "add_to_cart", "purchase", "logout", "view_product"]},
        "product_id": {"type": "string"},
        "device_type": {"type": "string", "enum": ["mobile", "desktop", "tablet"]},
        "browser": {"type": "string", "enum": ["Chrome", "Firefox", "Safari", "Edge", "Opera"]},
        "geo_location": {"type": "string"},
        "session_duration": {"type": "number", "minimum": 0},

        # Product/Inventory elements
        "product_name": {"type": "string"},
        "category": {"type": "string", "enum": ["Electronics", "Fashion", "Home & Kitchen", "Phones & Tablets", "Computers", "Health & Beauty", "Sporting Goods", "Automotive", "Baby Products", "Groceries"]},
        "price": {"type": "number", "minimum": 0.01},
        "discount": {"type": "number", "minimum": 0, "maximum": 100},
        "vendor_id": {"type": "string"},
        # For multi-item orders (list of line items)
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "product_id": {"type": "string"},
                    "product_name": {"type": "string"},
                    "category": {"type": "string"},
                    "price": {"type": "number", "minimum": 0.01},
                    "quantity": {"type": "integer", "minimum": 1},
                    "discount": {"type": "number", "minimum": 0, "maximum": 100},
                    "vendor_id": {"type": "string"},
                    "item_total": {"type": "number", "minimum": 0}
                },
                "required": ["product_id", "price", "quantity", "item_total"]
            }
        },

        # Transaction/Payment elements
        "transaction_id": {"type": "string"},
        "order_id": {"type": "string"},
        "quantity": {"type": "integer", "minimum": 1},
        "payment_method": {"type": "string", "enum": ["Card", "Bank Transfer", "USSD", "Mobile Money", "Pay on Delivery"]},
        "payment_status": {"type": "string", "enum": ["success", "failed", "pending", "refunded"]},
        "amount": {"type": "number", "minimum": 0.01},
        "currency": {"type": "string", "enum": ["NGN", "USD"]},
        "delivery_address": {"type": "string"},

        # Feedback/Post-Transaction elements
        "return_request": {"type": "boolean"},
        "refund_status": {"type": "string", "enum": ["none", "requested", "approved", "rejected", "processed"]},
        "rating": {"type": "integer", "minimum": 1, "maximum": 5},

        # Fraud detection target
        # Keep is_fraud as integer (0/1) to match generator usage; could be boolean as an alternative.
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1},
        "fraud_type": {"type": "string", "enum": ["none", "account_takeover", "card_testing", "friendly_fraud", "merchant_collusion", "geo_anomaly", "identity_theft"]},

        # Extended feature set for model training (ELEMENTS FOR MODEL TRAINING)
        "account_age_days": {"type": "integer", "minimum": 0},
        "identity_mismatch_score": {"type": "number", "minimum": 0, "maximum": 1},
        "identity_verification_score": {"type": "number", "minimum": 0, "maximum": 1},
        "SIM_swap_probability": {"type": "number", "minimum": 0, "maximum": 1},
        "NIN_verification_status": {"type": "string"},
        "phone_verified": {"type": "boolean"},
        "sim_card_age_days": {"type": "integer", "minimum": 0},
        "email_disposable_flag": {"type": "boolean"},
        "device_fingerprint_score": {"type": "number", "minimum": 0, "maximum": 1},
        "android_emulator_detection": {"type": "boolean"},

        "unusual_discount_flag": {"type": "boolean"},
        "product_price_anomaly_score": {"type": "number", "minimum": 0, "maximum": 1},
        "cart_tampering_detection": {"type": "boolean"},
        "discount_velocity_24h": {"type": "integer", "minimum": 0},
        "browser_integrity_score": {"type": "number", "minimum": 0, "maximum": 1},

        "transaction_velocity_1h": {"type": "integer", "minimum": 0},
        "transaction_velocity_24h": {"type": "integer", "minimum": 0},
        "payment_velocity_1h": {"type": "number", "minimum": 0},
        "payment_velocity_24h": {"type": "number", "minimum": 0},
        "failed_payment_ratio": {"type": "number", "minimum": 0, "maximum": 1},
        "avg_order_value": {"type": "number", "minimum": 0},
        "current_order_value_ratio": {"type": "number", "minimum": 0},
        "payment_method_anomaly": {"type": "boolean"},
        "is_linked_card": {"type": "boolean"},
        "bank_transfer_verification": {"type": "boolean"},
        "partial_payment_requirement": {"type": "boolean"},
        "address_risk_score": {"type": "number", "minimum": 0, "maximum": 1},

        "ip_location_mismatch_flag": {"type": "boolean"},
        "location_distance_score": {"type": "number", "minimum": 0},
        "address_reuse_rate": {"type": "number", "minimum": 0},
        "delivery_delay_ratio": {"type": "number", "minimum": 0, "maximum": 1},
        "failed_delivery_pattern": {"type": "boolean"},
        "shared_address_count": {"type": "integer", "minimum": 0},
        "rider_collusion_risk": {"type": "number", "minimum": 0, "maximum": 1},
        "otp_delivery_verification": {"type": "boolean"},
        "gps_delivery_confirmation": {"type": "boolean"},

        "refund_abuse_score": {"type": "number", "minimum": 0, "maximum": 1},
        "refund_to_purchase_ratio": {"type": "number", "minimum": 0},
        "refund_timing_pattern": {"type": "string"},
        "complaint_frequency": {"type": "integer", "minimum": 0},
        "delivery_dispute_rate": {"type": "number", "minimum": 0, "maximum": 1},
        "serial_number_mismatch": {"type": "boolean"},
        "return_inspection_score": {"type": "number", "minimum": 0, "maximum": 1},
        "product_swap_probability": {"type": "number", "minimum": 0, "maximum": 1},

        "fraud_ring_score": {"type": "number", "minimum": 0, "maximum": 1},
        "account_link_density": {"type": "number", "minimum": 0},
        "shared_phone_number_count": {"type": "integer", "minimum": 0},
        "shared_ip_count": {"type": "integer", "minimum": 0},
        "linked_accounts": {"type": "integer", "minimum": 0},
        "device_change_rate": {"type": "number", "minimum": 0},
        "multi_device_login_flag": {"type": "boolean"},
        "proxy_detection_flag": {"type": "boolean"},
        "VPN_usage_flag": {"type": "boolean"},
        "device_trust_score": {"type": "number", "minimum": 0, "maximum": 1},
        "ip_risk_score": {"type": "number", "minimum": 0, "maximum": 1},
        "user_reputation_score": {"type": "number", "minimum": 0, "maximum": 1},
        "activity_timing_score": {"type": "number", "minimum": 0},
        "anomaly_frequency_score": {"type": "number", "minimum": 0},
        "login_frequency": {"type": "integer", "minimum": 0},
        "behavioral_consistency_score": {"type": "number", "minimum": 0, "maximum": 1},

        "vendor_risk_score": {"type": "number", "minimum": 0, "maximum": 1},
        "vendor_age_days": {"type": "integer", "minimum": 0},
        "fake_review_score": {"type": "number", "minimum": 0, "maximum": 1},
        "rating_burst_detection": {"type": "boolean"},
        "seller_fraud_pattern": {"type": "string"}
    },
    "required": ["user_id", "transaction_id", "timestamp", "amount", "is_fraud"]
}


class EcommerceTransactionProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_username = os.getenv("KAFKA_USERNAME")
        self.kafka_password = os.getenv("KAFKA_PASSWORD")
        self.topic = os.getenv("KAFKA_TOPIC", "ecommerce_transactions")
        self.running = False

        # Log connection info (do not log secrets)
        logger.debug("Kafka Username: %s", self.kafka_username)
        logger.debug("Bootstrap Servers: %s", self.bootstrap_servers)
        logger.debug("Kafka Topic: %s", self.topic)

        # Producer configuration
        self.producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "ecommerce-transaction-producer",
            "compression.type": "gzip",
            "linger.ms": 5,
            "batch.size": 16384,
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.kafka_username,
                "sasl.password": self.kafka_password,
            })
        else:
            self.producer_config["security.protocol"] = "PLAINTEXT"

        try:
            self.producer = Producer(self.producer_config)
            logger.info("E-commerce Kafka Producer initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {str(e)}")
            raise e

        # Attempt to initialize DB engine if DB env vars provided
        # Lazy import to avoid importing SQLAlchemy at module import time (helps local testing)
        try:
            from utils.db import get_engine, create_transactions_table, insert_transaction as _insert_transaction
            self.db_engine = get_engine()
            create_transactions_table(self.db_engine)
            self.insert_transaction = _insert_transaction
            logger.info("Connected to Postgres and ensured transactions table exists.")
        except Exception as e:
            logger.warning("Postgres not configured or unreachable (skipping DB): %s", e)
            self.db_engine = None
            self.insert_transaction = None

        # Nigerian-specific data
        self.nigerian_states = ['Lagos', 'Abuja', 'Kano', 'Rivers', 'Delta', 'Oyo', 'Kaduna', 'Edo', 'Enugu', 'Anambra']
        self.nigerian_cities = ['Lagos', 'Abuja', 'Kano', 'Port Harcourt', 'Ibadan', 'Benin City', 'Enugu', 'Aba', 'Onitsha', 'Warri']
        
        # Fraud patterns specific to Nigerian e-commerce
        self.compromised_users = set(random.sample(range(1000, 9999), 50))
        self.high_risk_vendors = ['QuickDealsNG', 'NaijaMarket', 'JumiaFlash', 'KongaDeals']
        self.suspicious_domains = ['tempmail.ng', 'fakebox.com', 'mailinator.com']
        
        # Nigerian payment methods
        self.payment_methods = ["Card", "Bank Transfer", "USSD", "Mobile Money", "Pay on Delivery"]
        
        # Product categories with typical Nigerian pricing
        self.product_categories = {
            "Electronics": (5000, 500000),
            "Fashion": (1000, 50000),
            "Phones & Tablets": (15000, 300000),
            "Computers": (50000, 800000),
            "Home & Kitchen": (2000, 100000),
            "Health & Beauty": (500, 25000),
            "Groceries": (500, 20000)
        }

        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        
        # User profiles cache
        self.user_profiles = self._generate_user_profiles(1000)

        # Optional local file sink configuration (for debugging / offline ingestion)
        # Local file sink removed

    def _generate_user_profiles(self, count: int) -> Dict[str, Dict]:
        """Generate realistic user profiles for Nigerian users"""
        profiles = {}
        for i in range(count):
            user_id = f"USER{1000 + i}"
            registration_date = fake.date_time_between(start_date='-2y', end_date='now', tzinfo=timezone.utc)
            
            profiles[user_id] = {
                "user_id": user_id,
                "full_name": fake.name(),
                "email": fake.email(),
                "phone_number": f"+234{random.randint(7000000000, 8099999999)}",
                "gender": random.choice(["Male", "Female"]),
                "age": random.randint(18, 65),
                "registration_date": registration_date.isoformat(),
                "account_status": random.choices(["active", "suspended", "inactive"], weights=[0.85, 0.05, 0.1])[0],
                "device_id": f"DEV{random.randint(10000, 99999)}",
                "location": random.choice(self.nigerian_cities),
                "country": "Nigeria",
                "typical_device": random.choice(["mobile", "desktop", "tablet"]),
                "typical_browser": random.choice(["Chrome", "Firefox", "Safari"]),
                "typical_payment_method": random.choice(self.payment_methods)
            }
        return profiles

    def _generate_nigerian_ip(self) -> str:
        """Generate Nigerian IP addresses"""
        return f"197.210.{random.randint(0, 255)}.{random.randint(0, 255)}"

    def _generate_suspicious_ip(self) -> str:
        """Generate suspicious non-Nigerian IPs for fraud patterns"""
        suspicious_ranges = [
            f"185.100.{random.randint(0, 255)}.{random.randint(0, 255)}",  # Some European ranges
            f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",  # Private network
            f"10.0.{random.randint(0, 255)}.{random.randint(0, 255)}",     # Private network
        ]
        return random.choice(suspicious_ranges)

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate transaction against JSON schema"""
        try:
            validate(
                instance=transaction,
                schema=ECOMMERCE_TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f"Invalid transaction: {e.message}")
            return False

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        """Generate complete e-commerce transaction with Nigerian context"""
        # Select a user profile
        user_id = random.choice(list(self.user_profiles.keys()))
        user_profile = self.user_profiles[user_id]
        
        # Generate product details
        category = random.choice(list(self.product_categories.keys()))
        min_price, max_price = self.product_categories[category]
        price = round(random.uniform(min_price, max_price), 2)
        # Base transaction
        # Ensure quantity is generated once and used to compute amount = price * quantity
        qty = random.randint(1, 5)
        transaction = {
            # Customer/Profile elements
            **user_profile,

            # Session/Behavioral elements
            "session_id": f"SESS{random.randint(10000, 99999)}",
            "timestamp": (datetime.now(timezone.utc) +
                         timedelta(seconds=random.randint(-300, 300))).isoformat(),
            "action": random.choice(["login", "browse", "add_to_cart", "purchase", "view_product"]),
            "product_id": f"PROD{random.randint(1000, 9999)}",
            "device_type": user_profile["typical_device"],
            "browser": user_profile["typical_browser"],
            "geo_location": f"{user_profile['location']}, Nigeria",
            "session_duration": random.randint(30, 1800),  # 30 seconds to 30 minutes

            # Product/Inventory elements
            "product_name": f"{category} Product {random.randint(1, 100)}",
            "category": category,
            "price": price,
            "discount": random.choices([0, 5, 10, 15, 20, 30, 50], weights=[0.4, 0.2, 0.15, 0.1, 0.08, 0.05, 0.02])[0],
            "vendor_id": random.choice([f"VEND{random.randint(100, 999)}"] + self.high_risk_vendors),

            # Transaction/Payment elements
            "transaction_id": f"TXN{random.randint(100000, 999999)}",
            "order_id": f"ORD{random.randint(100000, 999999)}",
            "quantity": qty,
            "payment_method": user_profile["typical_payment_method"],
            "payment_status": "success",
            # Ensure amount reflects price * quantity
            "amount": round(price * qty, 2),
            "currency": "NGN",
            "delivery_address": f"{random.randint(1, 999)} {fake.street_name()}, {user_profile['location']}",

            # Feedback/Post-Transaction elements
            "return_request": False,
            "refund_status": "none",
            "rating": random.randint(1, 5),

            # Fraud detection
            "is_fraud": 0,
            "fraud_type": "none"
        }

        # Populate extended feature set derived from the ELEMENT FOR MODEL TRAINING
        # Account / identity
        reg_dt = datetime.fromisoformat(user_profile["registration_date"])
        account_age_days = max(0, (datetime.now(timezone.utc) - reg_dt).days)
        transaction.update({
            "account_age_days": account_age_days,
            "identity_mismatch_score": round(random.random(), 3),
            "identity_verification_score": round(random.random(), 3),
            "SIM_swap_probability": round(random.random() * 0.2, 3),
            "NIN_verification_status": random.choice(["verified", "unverified", "pending"]),
            "phone_verified": random.random() < 0.9,
            "sim_card_age_days": random.randint(30, 4000),
            "email_disposable_flag": any(d in transaction["email"] for d in self.suspicious_domains),
            "device_fingerprint_score": round(random.random(), 3),
            "android_emulator_detection": random.random() < 0.02,
        })

        # Product / cart
        transaction.update({
            "unusual_discount_flag": transaction["discount"] >= 30,
            "product_price_anomaly_score": round(random.random(), 3),
            "cart_tampering_detection": random.random() < 0.01,
            "discount_velocity_24h": random.randint(0, 5),
            "browser_integrity_score": round(random.random(), 3),
        })

        # Payment processing
        transaction.update({
            "transaction_velocity_1h": random.randint(0, 5),
            "transaction_velocity_24h": random.randint(0, 20),
            "payment_velocity_1h": round(random.uniform(0, 500000), 2),
            "payment_velocity_24h": round(random.uniform(0, 2000000), 2),
            "failed_payment_ratio": round(random.random() * 0.2, 3),
            "avg_order_value": round(random.uniform(1000, 200000), 2),
            "current_order_value_ratio": round(transaction["amount"] / max(1, random.uniform(1000, 50000)), 3),
            "payment_method_anomaly": random.random() < 0.02,
            "is_linked_card": random.random() < 0.15,
            "bank_transfer_verification": random.random() < 0.95,
            "partial_payment_requirement": transaction["amount"] > 100000,
            "address_risk_score": round(random.random(), 3),
        })

        # Delivery & logistics
        transaction.update({
            "ip_location_mismatch_flag": random.random() < 0.03,
            "location_distance_score": round(random.uniform(0, 1000), 2),
            "address_reuse_rate": round(random.random(), 3),
            "delivery_delay_ratio": round(random.random(), 3),
            "failed_delivery_pattern": random.random() < 0.02,
            "shared_address_count": random.randint(1, 4),
            "rider_collusion_risk": round(random.random(), 3),
            "otp_delivery_verification": random.random() < 0.95,
            "gps_delivery_confirmation": random.random() < 0.9,
        })

        # Returns & refunds
        transaction.update({
            "refund_abuse_score": round(random.random(), 3),
            "refund_to_purchase_ratio": round(random.random(), 3),
            "refund_timing_pattern": random.choice(["immediate", "within_week", "delayed", "none"]),
            "complaint_frequency": random.randint(0, 5),
            "delivery_dispute_rate": round(random.random(), 3),
            "serial_number_mismatch": random.random() < 0.005,
            "return_inspection_score": round(random.random(), 3),
            "product_swap_probability": round(random.random(), 3),
        })

        # Behavioral & network
        transaction.update({
            "fraud_ring_score": round(random.random(), 3),
            "account_link_density": round(random.random(), 3),
            "shared_phone_number_count": random.randint(0, 3),
            "shared_ip_count": random.randint(0, 5),
            "linked_accounts": random.randint(0, 3),
            "device_change_rate": round(random.random(), 3),
            "multi_device_login_flag": random.random() < 0.05,
            "proxy_detection_flag": random.random() < 0.02,
            "VPN_usage_flag": random.random() < 0.02,
            "device_trust_score": round(random.random(), 3),
            "ip_risk_score": round(random.random(), 3),
            "user_reputation_score": round(random.random(), 3),
            "activity_timing_score": round(random.random(), 3),
            "anomaly_frequency_score": round(random.random(), 3),
            "login_frequency": random.randint(0, 10),
            "behavioral_consistency_score": round(random.random(), 3),
        })

        # Vendor & marketplace
        transaction.update({
            "vendor_risk_score": round(random.random(), 3),
            "vendor_age_days": random.randint(0, 4000),
            "fake_review_score": round(random.random(), 3),
            "rating_burst_detection": random.random() < 0.01,
            "seller_fraud_pattern": random.choice(["none", "history_of_charges", "sudden_spike", "multiple_returns"]) 
        })

        # Apply Nigerian-specific fraud patterns
        transaction = self._apply_fraud_patterns(transaction, user_id)
        
        # Validate the complete transaction
        if self.validate_transaction(transaction):
            return transaction
        return None

    def _apply_fraud_patterns(self, transaction: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        """Apply realistic Nigerian e-commerce fraud patterns"""
        is_fraud = 0
        fraud_type = "none"
        amount = transaction['amount']
        
        # Pattern 1: Account Takeover (compromised credentials)
        if int(user_id[4:]) in self.compromised_users and transaction['action'] == 'purchase':
            if random.random() < 0.4:
                is_fraud = 1
                fraud_type = "account_takeover"
                transaction['device_type'] = random.choice(["mobile", "desktop"])  # Different device
                transaction['browser'] = random.choice(["Chrome", "Firefox"])  # Different browser
                transaction['ip_address'] = self._generate_suspicious_ip()
                transaction['amount'] = random.uniform(50000, 300000)  # High value
                transaction['payment_method'] = "Card"  # Preferred for takeovers
        
        # Pattern 2: Card Testing (small rapid transactions)
        elif (not is_fraud and amount < 2000 and 
              transaction['payment_method'] == "Card" and
              transaction['action'] == 'purchase'):
            if random.random() < 0.3:
                is_fraud = 1
                fraud_type = "card_testing"
                transaction['amount'] = round(random.uniform(100, 2000), 2)
                transaction['quantity'] = 1
        
        # Pattern 3: High-Risk Vendor Collusion
        elif (not is_fraud and transaction['vendor_id'] in self.high_risk_vendors):
            if random.random() < 0.25:
                is_fraud = 1
                fraud_type = "merchant_collusion"
                transaction['amount'] = random.uniform(30000, 150000)
                transaction['discount'] = 0  # No discounts on fraudulent items
        
        # Pattern 4: Geographic Anomalies
        elif (not is_fraud and random.random() < 0.1):
            is_fraud = 1
            fraud_type = "geo_anomaly"
            transaction['ip_address'] = self._generate_suspicious_ip()
            transaction['location'] = random.choice(self.nigerian_cities)
            transaction['geo_location'] = f"Unknown Location"
        
        # Pattern 5: New Account Fraud (recent registration + high value)
        registration_date = datetime.fromisoformat(transaction['registration_date'])
        days_since_registration = (datetime.now(timezone.utc) - registration_date).days
        
        if (not is_fraud and days_since_registration < 7 and amount > 50000 and
            transaction['action'] == 'purchase'):
            if random.random() < 0.4:
                is_fraud = 1
                fraud_type = "identity_theft"
        
        # Pattern 6: Friendly Fraud (legit customer claims non-delivery)
        if (not is_fraud and transaction['action'] == 'purchase' and 
            random.random() < 0.05):
            is_fraud = 1
            fraud_type = "friendly_fraud"
            transaction['return_request'] = True
            transaction['refund_status'] = "requested"
            transaction['rating'] = 1  # Low rating despite "issue"
        
        # Baseline random fraud
        if not is_fraud and random.random() < 0.015:  # 1.5% baseline
            is_fraud = 1
            fraud_type = random.choice(["account_takeover", "card_testing", "identity_theft"])
        
        # Ensure realistic fraud rate (1-3%)
        final_fraud = is_fraud if random.random() < 0.98 else 0
        
        transaction['is_fraud'] = final_fraud
        transaction['fraud_type'] = fraud_type if final_fraud else "none"
        
        # Update IP address for all transactions
        transaction['ip_address'] = (self._generate_suspicious_ip() if final_fraud and fraud_type in ["geo_anomaly", "account_takeover"] 
                                   else self._generate_nigerian_ip())
        
        return transaction

    

    def delivery_report(self, err, msg):
        """Delivery callback for confirming message delivery"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_transaction(self) -> bool:
        """Send a single transaction to Kafka with error handling"""
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False

            self.producer.produce(
                self.topic,
                key=transaction["transaction_id"],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )

            self.producer.poll(0)  # Trigger callbacks
            # Persist to Postgres if available (existing functionality)
            if getattr(self, "db_engine", None):
                insert_fn = getattr(self, "insert_transaction", None)
                if insert_fn:
                    try:
                        insert_fn(self.db_engine, transaction)
                    except Exception:
                        logger.exception("Failed to write transaction to Postgres")



            return True

        except Exception as e:
            logger.error(f"Error producing message: {str(e)}")
            return False

    def run_continuous_production(self, interval: float = 0.0):
        """Run continuous message production with graceful shutdown"""
        self.running = True
        logger.info("Starting producer for topic %s...", self.topic)

        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown procedure"""
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False
            if self.producer:
                self.producer.flush(timeout=30)  # <-- Ensure flush() is called
                self.producer.close()
            # Local file sink removed
            logger.info("Producer stopped")


if __name__ == "__main__":
    producer = EcommerceTransactionProducer()
    producer.run_continuous_production(interval=0.23)  # 23 transactions per second