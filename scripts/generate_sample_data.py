"""
scripts/generate_sample_data.py

Generates synthetic structured (CSV) and unstructured (plain-text) customer
review data for testing the Customer Feedback Analyzer pipeline.

Outputs
-------
  structured reviews  → <output_dir>/text_reviews_<ts>.csv
                         Matches the Amazon product-review schema consumed by
                         text_validator / text_normalizer / comprehend_processor.
                         S3 prefix: text-reviews/

  unstructured reviews → <output_dir>/unstructured_<ts>_<n>.txt
                         One free-form customer review per file, simulating
                         transcribed audio or raw text.
                         S3 prefix: text-reviews/

Usage
-----
  # Generate all types locally (default 50 records each)
  python scripts/generate_sample_data.py

  # Custom count and output directory
  python scripts/generate_sample_data.py --count 200 --output-dir /tmp/cfa_data

  # Generate only structured reviews and upload to S3
  python scripts/generate_sample_data.py --types structured --upload
"""
from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed data pools
# ---------------------------------------------------------------------------

_CATEGORIES = [
    "Electronics|Smartphones",
    "Electronics|Laptops",
    "Electronics|Headphones",
    "Computers&Accessories|Cables",
    "Home&Kitchen|Coffee Makers",
    "Home&Kitchen|Air Purifiers",
    "Sports&Outdoors|Fitness Trackers",
    "Books|Technical",
    "Clothing|Casual Wear",
    "Beauty|Skincare",
]

_PRODUCTS = [
    ("ProCharge X3 USB-C Cable 2m", "Electronics|Cables", "$4.99", "$9.99", "50%"),
    ("NoiseX Pro Wireless Earbuds", "Electronics|Headphones", "$19.99", "$34.99", "43%"),
    ("SwiftBook Air 14 Laptop", "Electronics|Laptops", "$549.99", "$699.99", "21%"),
    ("AquaFlow Smart Water Purifier", "Home&Kitchen|Appliances", "$84.99", "$119.99", "29%"),
    ("FitPulse Smartwatch Gen 2", "Electronics|Wearables", "$32.99", "$49.99", "34%"),
    ("CoolBreeze Mini Desk Fan", "Home&Kitchen|Fans", "$7.99", "$11.99", "33%"),
    ("LumiGlow LED Desk Lamp", "Home&Kitchen|Lighting", "$12.99", "$24.99", "48%"),
    ("GripMax Portable Power Bank 20000mAh", "Electronics|Power Banks", "$14.99", "$29.99", "50%"),
    ("BreathEasy Air Purifier HEPA", "Home&Kitchen|Air Purifiers", "$69.99", "$99.99", "30%"),
    ("StreamDeck Webcam 1080p", "Electronics|Webcams", "$24.99", "$39.99", "37%"),
]

_POSITIVE_TITLES = [
    "Excellent product, highly recommend",
    "Worth every dollar",
    "Exceeded my expectations",
    "Great quality and fast delivery",
    "Absolutely love it",
    "Best purchase this year",
    "Works perfectly out of the box",
    "Outstanding build quality",
    "Very happy with this purchase",
    "Solid product, no complaints",
]

_MIXED_TITLES = [
    "Good but has some issues",
    "Decent for the price",
    "Works fine, minor quibbles",
    "Mostly satisfied",
    "Average product",
    "Okay for casual use",
    "Not bad, could be better",
    "Acceptable quality at this price",
]

_NEGATIVE_TITLES = [
    "Very disappointed",
    "Stopped working after two weeks",
    "Not worth the price",
    "Poor quality control",
    "Misleading product description",
    "Would not recommend",
    "Returned immediately",
    "Broke within days",
]

_POSITIVE_CONTENT = [
    "I have been using this for about a month now and I am genuinely impressed. The build quality is exceptional and it performs exactly as advertised. Delivery was prompt and packaging was secure. Would definitely recommend to friends and family.",
    "Superb product at an affordable price point. I was skeptical initially but it has surpassed all my expectations. The performance is rock-solid and I have had zero issues since purchase.",
    "After researching for weeks I finally settled on this and I am so glad I did. The quality is top-notch, setup was straightforward, and customer support answered my questions within hours. Five stars without hesitation.",
    "This is exactly what I needed. Fast charging, durable cable, fits the case perfectly. I have bought three for different family members already. Highly recommended!",
    "Outstanding product. The battery life is incredible and the sound quality is rich with good bass response. Connects instantly every time. Very comfortable for long sessions too.",
    "Arrived two days early, packed well, and works flawlessly. I have been a loyal customer of this brand for years and this product continues to impress. Keep up the good work.",
]

_MIXED_CONTENT = [
    "Overall a decent product for everyday use. The performance is satisfactory but I did notice occasional lag when multitasking. For the price point I think it is a fair trade-off. The build feels slightly plasticky compared to premium alternatives but nothing deal-breaking.",
    "Works as described but the setup instructions could be clearer. Took me about 30 minutes to figure out the initial configuration. Once up and running though it performs reliably. Packaging could also be improved.",
    "The core functionality is great but some features feel half-baked. The companion app frequently needs relaunching and Bluetooth connectivity can be spotty. Hoping future updates address these. Main unit hardware is solid enough.",
    "Good value for money but do not expect premium experience. Materials feel average and one of the accessory pieces broke after a month of normal use. Customer service was helpful but slow to respond.",
]

_NEGATIVE_CONTENT = [
    "Deeply disappointed with this purchase. The product stopped functioning after just ten days of light use. The build quality is far below what the product images suggest. Raised a return request immediately.",
    "This is a complete waste of money. The item arrived with a dent and the connector was damaged. Customer support took four days to respond and the refund process is a nightmare. Never buying from this brand again.",
    "The charging speed is nowhere near the advertised 65W. At best I see 18W on my device. The cable also heats up considerably which is a safety concern. Very disappointed given the price.",
    "Product failed after two weeks. The buttons became unresponsive and the display started flickering. Quality control seems nonexistent. The packaging promises a lot but the product delivers very little.",
]

_USER_NAMES = [
    "Aditya S", "Priya M", "Rahul K", "Swati R", "Vijay N",
    "Meera J", "Sanjay P", "Deepa T", "Arjun L", "Kavita D",
    "Rohan B", "Neha G", "Suresh V", "Anita C", "Kiran H",
    "Mohan R", "Pooja S", "Nitin K", "Ananya M", "Deepak A",
]

_UNSTRUCTURED_REVIEWS = [
    "I recently bought this product and I absolutely love how it performs every day. The build quality is exactly what you would expect at this price point and I have had zero issues so far.",
    "Honestly not happy with my purchase. The item looked completely different from the photos and the quality feels really cheap. I would not recommend this to anyone.",
    "Pretty decent product. It does its job but nothing more. If you are looking for something budget-friendly this will work. Do not expect flagship quality for this price.",
    "Wow I am genuinely surprised by how good this is. I ordered it without much research and it turned out to be one of the best impulse buys I have made. The performance is smooth and reliable.",
    "The first unit I received was faulty. Customer service replaced it quickly and the second one works flawlessly. Five stars for the after-sales support even if the initial quality control could be better.",
    "After three months of daily use I can confidently say this is worth the money. The battery has not degraded noticeably and performance remains consistent. Very pleased.",
    "This product solved a problem I have been struggling with for over a year. Simple to set up, intuitive to use, and the results speak for themselves. Highly recommended.",
    "Not impressed at all. My old unit from a different brand was significantly better. The design is fine but the internals feel rushed and the software is full of bugs.",
    "Solid product with great value. There are minor cosmetic imperfections on my unit but functionally it is perfect. Happy to overlook the aesthetics for this price.",
    "The product does exactly what the description says, nothing more nothing less. Good for basic everyday use. Would buy again if this one ever fails.",
    "Called customer support twice about a connectivity problem and they were extremely helpful. Walked me through the troubleshooting steps and the issue is now completely resolved.",
    "Ordered as a birthday gift. The recipient absolutely loved it. Came in a nice box, looked premium, and works exactly as expected. Definitely buying from this seller again.",
    "Had high hopes based on the reviews but was let down. Build quality feels flimsy and the product runs hotter than I expected during normal operation. Concerned about longevity.",
    "Fantastic product. Replaced an older model with this and the difference is night and day. The new design is more ergonomic and the performance is noticeably faster.",
    "Works for what I need it for. Nothing fancy. Does the job and the price is reasonable. Packaging could be less wasteful though as there was a lot of unnecessary plastic.",
]


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def _uid(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12].upper()}"


def generate_structured_reviews(count: int) -> list[dict]:
    """Generate ``count`` rows in the Amazon product-review CSV schema."""
    rows: list[dict] = []
    for _ in range(count):
        product = random.choice(_PRODUCTS)
        name, category, disc_price, act_price, disc_pct = product
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 12, 35, 40])[0]

        if rating >= 4:
            title = random.choice(_POSITIVE_TITLES)
            content = random.choice(_POSITIVE_CONTENT)
        elif rating == 3:
            title = random.choice(_MIXED_TITLES)
            content = random.choice(_MIXED_CONTENT)
        else:
            title = random.choice(_NEGATIVE_TITLES)
            content = random.choice(_NEGATIVE_CONTENT)

        rows.append({
            "product_id": _uid("B0"),
            "product_name": name,
            "category": category,
            "discounted_price": disc_price,
            "actual_price": act_price,
            "discount_percentage": disc_pct,
            "rating": rating,
            "rating_count": str(random.randint(100, 100_000)),
            "about_product": f"High-quality {name.split()[0]} product designed for everyday use. Backed by manufacturer warranty.",
            "user_id": _uid("U"),
            "user_name": random.choice(_USER_NAMES),
            "review_id": _uid("R"),
            "review_title": title,
            "review_content": content,
            "img_link": "https://example.com/images/placeholder.jpg",
            "product_link": "https://example.com/products/placeholder",
        })
    return rows


_UNSTRUCTURED_OPENERS = [
    "I picked up the {name} a few weeks ago and wanted to share my thoughts.",
    "Just finished using the {name} for a while and here is my honest take.",
    "Bought the {name} recently — here is what I think.",
    "Writing this review after a couple of months with the {name}.",
    "My experience with the {name} has been worth sharing.",
    "I have been using the {name} daily and finally decided to write this up.",
    "Got the {name} as a gift and have some thoughts to share.",
    "After using the {name} for some time, here is my honest feedback.",
]


def generate_unstructured_reviews(count: int) -> list[str]:
    """Return ``count`` free-form review strings referencing a product from ``_PRODUCTS``."""
    pool = _UNSTRUCTURED_REVIEWS
    reviews: list[str] = []
    for i in range(count):
        product = random.choice(_PRODUCTS)
        name = product[0]
        opener = random.choice(_UNSTRUCTURED_OPENERS).format(name=name)
        base = pool[i % len(pool)]
        reviews.append(f"{opener} {base}")
    return reviews


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

_STRUCTURED_FIELDS = [
    "product_id", "product_name", "category", "discounted_price", "actual_price",
    "discount_percentage", "rating", "rating_count", "about_product",
    "user_id", "user_name", "review_id", "review_title", "review_content",
    "img_link", "product_link",
]

def write_structured_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_STRUCTURED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Structured reviews written → %s (%d rows)", path, len(rows))


def write_unstructured_txt(review: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(review.strip())
        f.write("\n")
    logger.info("Unstructured review written → %s", path)


# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------

def _resolve_input_bucket(prefix: str, profile: str) -> str:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    session = boto3.Session(profile_name=profile)
    try:
        cf = session.client("cloudformation")
        resp = cf.describe_stacks(StackName="CfaStorageStack")
        for output in resp["Stacks"][0].get("Outputs", []):
            if output["OutputKey"] == "InputBucketName":
                return output["OutputValue"]
    except (BotoCoreError, ClientError, KeyError):
        logger.debug("CloudFormation lookup failed", exc_info=True)

    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    return f"{prefix}-customer-feedback-input-{account_id}"


def upload_to_s3(
    local_path: Path,
    bucket: str,
    s3_key: str,
    content_type: str,
    profile: str,
) -> None:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")
    try:
        s3.upload_file(
            str(local_path),
            bucket,
            s3_key,
            ExtraArgs={"ContentType": content_type},
        )
        logger.info("Uploaded → s3://%s/%s", bucket, s3_key)
    except (BotoCoreError, ClientError) as exc:
        logger.error("Upload failed for %s: %s", local_path.name, exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic customer feedback data for the CFA pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["structured", "unstructured"],
        default=["structured", "unstructured"],
        metavar="TYPE",
        help="Data types to generate: structured unstructured (default: all)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of records to generate per type (default: 50)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).parent.parent / "sample_data"),
        help="Directory to write output files (default: sample_data/)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload generated files to the S3 input bucket after creation",
    )
    parser.add_argument(
        "--bucket",
        help="S3 bucket to upload to (auto-detected from CloudFormation if omitted)",
    )
    parser.add_argument(
        "--prefix",
        default="cfa",
        help="CDK resource prefix for auto-detecting bucket name (default: cfa)",
    )
    parser.add_argument(
        "--profile",
        default="aws_swami",
        help="AWS profile to use (default: aws_swami)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible output",
    )

    args = parser.parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)

    output_dir = Path(args.output_dir)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    generated: list[tuple[Path, str, str]] = []  # (local_path, s3_key, content_type)

    if "structured" in args.types:
        rows = generate_structured_reviews(args.count)
        path = output_dir / f"text_reviews_{ts}.csv"
        write_structured_csv(rows, path)
        generated.append((path, f"text-reviews/{path.name}", "text/csv"))

    if "unstructured" in args.types:
        reviews = generate_unstructured_reviews(args.count)
        for i, review in enumerate(reviews, start=1):
            path = output_dir / f"survey_unstructured_{ts}_{i:04d}.txt"
            write_unstructured_txt(review, path)
            generated.append((path, f"text-reviews/{path.name}", "text/plain"))
        logger.info("Unstructured reviews: %d individual files written", len(reviews))

    if not args.upload:
        logger.info(
            "Files saved locally. Run with --upload to push to S3, or use "
            "scripts/upload_sample_data.py for CSV files."
        )
        for local_path, s3_key, _ in generated:
            logger.info("  %s  (would go to  %s)", local_path.name, s3_key)
        return

    # Resolve bucket
    bucket = args.bucket
    if not bucket:
        try:
            bucket = _resolve_input_bucket(args.prefix, args.profile)
        except Exception as exc:
            logger.error("Could not resolve input bucket: %s", exc)
            logger.error("Pass --bucket explicitly or deploy CfaStorageStack first.")
            sys.exit(1)

    logger.info("Uploading to bucket: %s", bucket)
    for local_path, s3_key, content_type in generated:
        upload_to_s3(local_path, bucket, s3_key, content_type, args.profile)


if __name__ == "__main__":
    main()
