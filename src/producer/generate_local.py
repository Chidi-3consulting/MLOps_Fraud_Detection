import time
import argparse
import os
import sys
import importlib.util

# Import EcommerceTransactionProducer robustly whether running as script or module
try:
	# preferred when package import works (e.g., running from repo root with -m)
	from producer.main import EcommerceTransactionProducer
except Exception:
	# fallback: load the main.py module from the same directory
	this_dir = os.path.dirname(__file__)
	# Ensure parent directory (src/) is on sys.path so absolute imports in main.py work
	parent_dir = os.path.dirname(this_dir)
	if parent_dir not in sys.path:
		sys.path.insert(0, parent_dir)
	main_path = os.path.join(this_dir, 'main.py')
	spec = importlib.util.spec_from_file_location('producer.main', main_path)
	if spec is None or spec.loader is None:
		raise ImportError(f"Cannot load producer.main from {main_path}")
	main_mod = importlib.util.module_from_spec(spec)
	sys.modules['producer.main'] = main_mod
	spec.loader.exec_module(main_mod)  # type: ignore[attr-defined]
	EcommerceTransactionProducer = main_mod.EcommerceTransactionProducer


def main(count: int = 10, interval: float = 0.0):
	p = EcommerceTransactionProducer()
	sent = 0
	try:
		for _ in range(count):
			ok = p.send_transaction()
			sent += 1 if ok else 0
			if interval > 0:
				time.sleep(interval)
	finally:
		p.shutdown()
	print(f"Attempted: {count}, Successful sends: {sent}")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--count', '-n', type=int, default=10, help='Number of transactions to produce')
	parser.add_argument('--interval', '-i', type=float, default=0.0, help='Seconds between sends')
	args = parser.parse_args()
	main(count=args.count, interval=args.interval)
