"""Static reference data for the simulated marketplace."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    """A delivery city with its state and the leading digits of its pincodes."""

    name: str
    state: str
    state_code: str
    pincode_prefix: str
    weight: int
    former_name: str | None = None


CITIES: tuple[City, ...] = (
    City("Mumbai", "Maharashtra", "MH", "400", 14, "Bombay"),
    City("Delhi", "Delhi", "DL", "110", 14),
    City("Bengaluru", "Karnataka", "KA", "560", 13, "Bangalore"),
    City("Hyderabad", "Telangana", "TS", "500", 9),
    City("Chennai", "Tamil Nadu", "TN", "600", 8, "Madras"),
    City("Kolkata", "West Bengal", "WB", "700", 7, "Calcutta"),
    City("Pune", "Maharashtra", "MH", "411", 7),
    City("Ahmedabad", "Gujarat", "GJ", "380", 5),
    City("Gurugram", "Haryana", "HR", "122", 4, "Gurgaon"),
    City("Noida", "Uttar Pradesh", "UP", "201", 4),
    City("Jaipur", "Rajasthan", "RJ", "302", 3),
    City("Lucknow", "Uttar Pradesh", "UP", "226", 3),
    City("Surat", "Gujarat", "GJ", "395", 3),
    City("Kochi", "Kerala", "KL", "682", 3, "Cochin"),
    City("Chandigarh", "Chandigarh", "CH", "160", 2),
    City("Indore", "Madhya Pradesh", "MP", "452", 2),
    City("Coimbatore", "Tamil Nadu", "TN", "641", 2),
    City("Nagpur", "Maharashtra", "MH", "440", 2),
    City("Bhopal", "Madhya Pradesh", "MP", "462", 2),
    City("Visakhapatnam", "Andhra Pradesh", "AP", "530", 2, "Vizag"),
    City("Patna", "Bihar", "BR", "800", 2),
    City("Vadodara", "Gujarat", "GJ", "390", 2, "Baroda"),
    City("Thiruvananthapuram", "Kerala", "KL", "695", 1, "Trivandrum"),
    City("Bhubaneswar", "Odisha", "OD", "751", 1),
    City("Guwahati", "Assam", "AS", "781", 1),
    City("Mysuru", "Karnataka", "KA", "570", 1, "Mysore"),
    City("Nashik", "Maharashtra", "MH", "422", 1),
    City("Ludhiana", "Punjab", "PB", "141", 1),
    City("Dehradun", "Uttarakhand", "UK", "248", 1),
    City("Kanpur", "Uttar Pradesh", "UP", "208", 1),
)

PAYMENT_METHODS: tuple[tuple[str, int], ...] = (
    ("UPI", 45),
    ("CARD", 22),
    ("COD", 18),
    ("NET_BANKING", 8),
    ("WALLET", 7),
)

LOYALTY_TIERS: tuple[tuple[str, int], ...] = (
    ("bronze", 60),
    ("silver", 25),
    ("gold", 11),
    ("platinum", 4),
)


@dataclass(frozen=True)
class Leaf:
    """A leaf category with the price band of the products sold in it (INR)."""

    name: str
    min_price: int
    max_price: int


@dataclass(frozen=True)
class Department:
    """A top-level category: its leaves, house brands and attribute vocabulary."""

    name: str
    leaves: tuple[Leaf, ...]
    brands: tuple[str, ...]
    attributes: dict[str, tuple[str, ...]]


DEPARTMENTS: tuple[Department, ...] = (
    Department(
        "Electronics",
        (
            Leaf("Smartphones", 7000, 90000),
            Leaf("Laptops", 25000, 150000),
            Leaf("Headphones", 500, 25000),
            Leaf("Televisions", 9000, 120000),
            Leaf("Smart Watches", 1500, 45000),
        ),
        ("Voltix", "Auralite", "Zenkor", "Nimbus", "Pixora", "Kairo"),
        {
            "color": ("Black", "Silver", "Blue", "Graphite"),
            "warranty_months": ("6", "12", "24"),
            "connectivity": ("Bluetooth 5.3", "Wi-Fi 6", "5G", "USB-C"),
        },
    ),
    Department(
        "Fashion",
        (
            Leaf("Men's Clothing", 300, 5000),
            Leaf("Women's Clothing", 300, 8000),
            Leaf("Footwear", 400, 7000),
            Leaf("Watches", 500, 20000),
            Leaf("Bags and Luggage", 400, 9000),
        ),
        ("Rangrez", "Urbane Loom", "Kashvi", "Northfold", "Tarang", "Mirai"),
        {
            "color": ("Navy", "Maroon", "Olive", "White", "Black", "Mustard"),
            "size": ("S", "M", "L", "XL", "Free Size"),
            "material": ("Cotton", "Linen", "Polyester", "Leather", "Denim"),
        },
    ),
    Department(
        "Home and Kitchen",
        (
            Leaf("Cookware", 300, 6000),
            Leaf("Furniture", 1500, 60000),
            Leaf("Home Decor", 150, 5000),
            Leaf("Bedding", 400, 6000),
            Leaf("Kitchen Appliances", 800, 25000),
        ),
        ("Gharana", "Stelloware", "Aangan", "Woodnest", "Kitchora"),
        {
            "material": ("Stainless Steel", "Sheesham Wood", "Cotton", "Ceramic", "Glass"),
            "color": ("Beige", "Teal", "Walnut", "White"),
            "pack_of": ("1", "2", "4", "6"),
        },
    ),
    Department(
        "Beauty and Personal Care",
        (
            Leaf("Skincare", 150, 3000),
            Leaf("Haircare", 120, 2500),
            Leaf("Makeup", 150, 4000),
            Leaf("Fragrances", 300, 8000),
            Leaf("Grooming", 200, 6000),
        ),
        ("Nirvaya", "Glowroot", "Ayuka", "Verdant", "Suvasa"),
        {
            "volume_ml": ("30", "50", "100", "200"),
            "skin_type": ("All", "Dry", "Oily", "Sensitive"),
            "is_vegan": ("true", "false"),
        },
    ),
    Department(
        "Grocery",
        (
            Leaf("Staples", 40, 1500),
            Leaf("Snacks", 20, 800),
            Leaf("Beverages", 30, 1200),
            Leaf("Dry Fruits", 150, 3000),
            Leaf("Spices", 30, 900),
        ),
        ("Annapurna Farms", "Desi Basket", "Swadloom", "Harvestry"),
        {
            "weight_grams": ("100", "250", "500", "1000", "5000"),
            "is_vegetarian": ("true",),
            "shelf_life_days": ("90", "180", "365"),
        },
    ),
    Department(
        "Books and Stationery",
        (
            Leaf("Fiction", 100, 900),
            Leaf("Non-fiction", 150, 1500),
            Leaf("Academic", 200, 2500),
            Leaf("Children's Books", 80, 800),
            Leaf("Office Supplies", 50, 2000),
        ),
        ("Pothi Press", "Inkwell House", "Vidya Prakashan", "Margin Notes"),
        {
            "language": ("English", "Hindi", "Tamil", "Bengali", "Marathi"),
            "binding": ("Paperback", "Hardcover"),
            "pages": ("96", "180", "320", "540"),
        },
    ),
    Department(
        "Sports and Fitness",
        (
            Leaf("Cricket", 200, 15000),
            Leaf("Fitness Equipment", 300, 40000),
            Leaf("Cycling", 500, 35000),
            Leaf("Yoga", 200, 4000),
            Leaf("Outdoor Gear", 300, 12000),
        ),
        ("Stridex", "Akhada", "Peakline", "Yogvan", "Boundary"),
        {
            "material": ("English Willow", "Rubber", "Alloy Steel", "Nylon", "EVA Foam"),
            "color": ("Red", "Black", "Neon Green", "Blue"),
            "skill_level": ("Beginner", "Intermediate", "Professional"),
        },
    ),
    Department(
        "Toys and Baby",
        (
            Leaf("Soft Toys", 150, 2500),
            Leaf("Board Games", 200, 3000),
            Leaf("Baby Care", 100, 3000),
            Leaf("Educational Toys", 200, 4000),
            Leaf("Strollers", 2000, 25000),
        ),
        ("Tinytots", "Khilona Co", "Bloomkin", "Playmitra"),
        {
            "age_group": ("0-2 years", "3-5 years", "6-8 years", "9+ years"),
            "material": ("Plush", "Wood", "BPA-free Plastic", "Cotton"),
            "color": ("Pink", "Yellow", "Sky Blue", "Multicolour"),
        },
    ),
)

REVIEW_PHRASES: dict[int, tuple[tuple[str, str], ...]] = {
    1: (
        ("Very disappointed", "Stopped working within a week and support did not respond."),
        ("Not as described", "The item looks nothing like the listing. Returning it."),
    ),
    2: (
        ("Below expectations", "Quality is poor for the price. Packaging was damaged too."),
        ("Would not buy again", "Arrived late and the finish feels cheap."),
    ),
    3: (
        ("It is okay", "Does the job but nothing special. Delivery took longer than promised."),
        ("Average product", "Decent for the price, though the build could be better."),
    ),
    4: (
        ("Good purchase", "Works well and arrived on time. Value for money."),
        ("Happy with it", "Good quality, matches the description. One small scratch on the box."),
    ),
    5: (
        ("Excellent", "Exactly what I wanted. Fast delivery and great packaging."),
        ("Highly recommended", "Superb quality at this price. Already ordered a second one."),
    ),
}

SELLER_REPLIES: tuple[str, ...] = (
    "Thank you for your feedback. We are glad you liked the product.",
    "We are sorry about your experience. Our team will contact you about a replacement.",
    "Thanks for shopping with us. Your review helps other buyers.",
)
