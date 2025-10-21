import json
import logging
import os
import random
import time
import signal
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from random import randint, choice

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker
from jsonschema import validate, ValidationError, FormatChecker

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
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
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1},
        "fraud_type": {"type": "string", "enum": ["none", "account_takeover", "card_testing", "friendly_fraud", "merchant_collusion", "geo_anomaly", "identity_theft"]}
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

        # Debug print to confirm credentials and topic
        print("Kafka Username:", self.kafka_username)
        print("Kafka Password:", self.kafka_password)
        print("Bootstrap Servers:", self.bootstrap_servers)
        print("Kafka Topic:", self.topic)

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
            "quantity": random.randint(1, 5),
            "payment_method": user_profile["typical_payment_method"],
            "payment_status": "success",
            "amount": price * random.randint(1, 3),
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
        """Send a single e-commerce transaction to Kafka"""
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False

            self.producer.produce(
                self.topic,
                key=transaction["transaction_id"],
                value=json.dumps(transaction, default=str),
                callback=self.delivery_report
            )

            self.producer.poll(0)
            return True

        except Exception as e:
            logger.error(f"Error producing message: {str(e)}")
            return False

    def run_continuous_production(self, interval: float = 0.1):
        """Run continuous message production"""
        self.running = True
        logger.info(f"Starting Nigerian e-commerce producer for topic {self.topic}...")
        logger.info(f"Target fraud rate: 1-3% with Nigerian-specific patterns")

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
                self.producer.flush(timeout=30)
                self.producer.close()
            logger.info("E-commerce producer stopped")

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
            logger.info("Producer stopped")


if __name__ == "__main__":
    producer = EcommerceTransactionProducer()
    producer.run_continuous_production(interval=0.05)  # ~20 transactions per second