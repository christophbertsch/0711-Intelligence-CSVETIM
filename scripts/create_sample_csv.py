#!/usr/bin/env python3
"""
Create sample CSV files for testing the CSV Import Guardian Agent System.
"""

import csv
import random
from pathlib import Path

# Sample data for different product types
FASTENER_DATA = [
    {
        "SKU": "SCR-001",
        "Name": "Phillips Head Screw M6x20",
        "Brand": "FastCorp",
        "GTIN": "1234567890123",
        "Length": "20",
        "Length_Unit": "mm",
        "Diameter": "6",
        "Diameter_Unit": "mm",
        "Material": "Stainless Steel",
        "Thread_Pitch": "1.0",
        "Head_Type": "Phillips",
        "Price": "0.15",
        "Currency": "EUR"
    },
    {
        "SKU": "SCR-002", 
        "Name": "Hex Head Bolt M8x30",
        "Brand": "FastCorp",
        "GTIN": "1234567890124",
        "Length": "30",
        "Length_Unit": "mm",
        "Diameter": "8",
        "Diameter_Unit": "mm", 
        "Material": "Steel",
        "Thread_Pitch": "1.25",
        "Head_Type": "Hex",
        "Price": "0.25",
        "Currency": "EUR"
    },
    {
        "SKU": "SCR-003",
        "Name": "Flat Head Screw M4x15",
        "Brand": "FastCorp", 
        "GTIN": "1234567890125",
        "Length": "15",
        "Length_Unit": "mm",
        "Diameter": "4",
        "Diameter_Unit": "mm",
        "Material": "Brass",
        "Thread_Pitch": "0.7",
        "Head_Type": "Flat",
        "Price": "0.12",
        "Currency": "EUR"
    }
]

ELECTRONICS_DATA = [
    {
        "PartNumber": "RES-001",
        "Description": "Carbon Film Resistor 1kΩ",
        "Manufacturer": "ElectroCorp",
        "EAN": "2345678901234",
        "Resistance": "1000",
        "Resistance_Unit": "Ω",
        "Power": "0.25",
        "Power_Unit": "W",
        "Tolerance": "5",
        "Tolerance_Unit": "%",
        "Package": "Axial",
        "Price": "0.05",
        "Currency": "EUR"
    },
    {
        "PartNumber": "CAP-001",
        "Description": "Ceramic Capacitor 100nF",
        "Manufacturer": "ElectroCorp",
        "EAN": "2345678901235", 
        "Capacitance": "100",
        "Capacitance_Unit": "nF",
        "Voltage": "50",
        "Voltage_Unit": "V",
        "Tolerance": "10",
        "Tolerance_Unit": "%",
        "Package": "0805",
        "Price": "0.03",
        "Currency": "EUR"
    },
    {
        "PartNumber": "LED-001",
        "Description": "Red LED 5mm",
        "Manufacturer": "ElectroCorp",
        "EAN": "2345678901236",
        "Color": "Red",
        "Diameter": "5",
        "Diameter_Unit": "mm",
        "Forward_Voltage": "2.1",
        "Voltage_Unit": "V",
        "Forward_Current": "20",
        "Current_Unit": "mA",
        "Package": "Through-hole",
        "Price": "0.08",
        "Currency": "EUR"
    }
]

def create_sample_files():
    """Create sample CSV files."""
    output_dir = Path("sample_data")
    output_dir.mkdir(exist_ok=True)
    
    # Create fastener sample
    fastener_file = output_dir / "fasteners_sample.csv"
    with open(fastener_file, 'w', newline='', encoding='utf-8') as f:
        if FASTENER_DATA:
            writer = csv.DictWriter(f, fieldnames=FASTENER_DATA[0].keys())
            writer.writeheader()
            
            # Write original data
            writer.writerows(FASTENER_DATA)
            
            # Generate additional rows
            for i in range(4, 101):  # Generate 97 more rows
                row = FASTENER_DATA[i % 3].copy()  # Cycle through base data
                row["SKU"] = f"SCR-{i:03d}"
                row["Name"] = f"Screw Type {i}"
                row["GTIN"] = f"123456789{i:04d}"
                row["Length"] = str(random.randint(10, 50))
                row["Diameter"] = str(random.choice([4, 5, 6, 8, 10]))
                row["Material"] = random.choice(["Steel", "Stainless Steel", "Brass", "Aluminum"])
                row["Price"] = f"{random.uniform(0.10, 0.50):.2f}"
                writer.writerow(row)
    
    print(f"Created fastener sample: {fastener_file}")
    
    # Create electronics sample
    electronics_file = output_dir / "electronics_sample.csv"
    with open(electronics_file, 'w', newline='', encoding='utf-8') as f:
        if ELECTRONICS_DATA:
            writer = csv.DictWriter(f, fieldnames=ELECTRONICS_DATA[0].keys())
            writer.writeheader()
            
            # Write original data
            writer.writerows(ELECTRONICS_DATA)
            
            # Generate additional rows
            for i in range(4, 51):  # Generate 47 more rows
                row = ELECTRONICS_DATA[i % 3].copy()
                row["PartNumber"] = f"PART-{i:03d}"
                row["Description"] = f"Electronic Component {i}"
                row["EAN"] = f"234567890{i:04d}"
                row["Price"] = f"{random.uniform(0.01, 1.00):.2f}"
                writer.writerow(row)
    
    print(f"Created electronics sample: {electronics_file}")
    
    # Create problematic sample (for testing validation)
    problematic_file = output_dir / "problematic_sample.csv"
    with open(problematic_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["SKU", "Name", "Brand", "GTIN", "Length", "Price"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Good row
        writer.writerow({
            "SKU": "GOOD-001",
            "Name": "Good Product",
            "Brand": "GoodBrand",
            "GTIN": "1234567890123",
            "Length": "25.5",
            "Price": "1.50"
        })
        
        # Missing required field
        writer.writerow({
            "SKU": "",  # Missing SKU
            "Name": "Missing SKU Product",
            "Brand": "TestBrand",
            "GTIN": "1234567890124",
            "Length": "30",
            "Price": "2.00"
        })
        
        # Invalid GTIN
        writer.writerow({
            "SKU": "BAD-001",
            "Name": "Invalid GTIN Product",
            "Brand": "TestBrand", 
            "GTIN": "invalid-gtin",  # Invalid format
            "Length": "20",
            "Price": "1.25"
        })
        
        # Negative length
        writer.writerow({
            "SKU": "BAD-002",
            "Name": "Negative Length Product",
            "Brand": "TestBrand",
            "GTIN": "1234567890125",
            "Length": "-5",  # Invalid negative value
            "Price": "0.75"
        })
        
        # Missing price
        writer.writerow({
            "SKU": "BAD-003",
            "Name": "Missing Price Product",
            "Brand": "TestBrand",
            "GTIN": "1234567890126", 
            "Length": "15",
            "Price": ""  # Missing price
        })
    
    print(f"Created problematic sample: {problematic_file}")
    
    # Create large sample (for performance testing)
    large_file = output_dir / "large_sample.csv"
    with open(large_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["SKU", "Name", "Brand", "GTIN", "Category", "Length", "Width", "Height", "Weight", "Price"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        categories = ["Fasteners", "Electronics", "Tools", "Hardware", "Components"]
        brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
        
        for i in range(1, 10001):  # 10,000 rows
            writer.writerow({
                "SKU": f"LARGE-{i:05d}",
                "Name": f"Product {i}",
                "Brand": random.choice(brands),
                "GTIN": f"999{i:010d}",
                "Category": random.choice(categories),
                "Length": f"{random.uniform(10, 100):.1f}",
                "Width": f"{random.uniform(5, 50):.1f}",
                "Height": f"{random.uniform(2, 20):.1f}",
                "Weight": f"{random.uniform(0.1, 5.0):.2f}",
                "Price": f"{random.uniform(0.50, 50.00):.2f}"
            })
    
    print(f"Created large sample: {large_file}")
    
    # Create different delimiter sample
    semicolon_file = output_dir / "semicolon_sample.csv"
    with open(semicolon_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ["Artikel", "Bezeichnung", "Marke", "EAN", "Preis"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        
        for i in range(1, 21):
            writer.writerow({
                "Artikel": f"ART-{i:03d}",
                "Bezeichnung": f"Produkt {i}",
                "Marke": "Deutsche Marke",
                "EAN": f"400{i:010d}",
                "Preis": f"{random.uniform(1.00, 20.00):.2f}"
            })
    
    print(f"Created semicolon delimited sample: {semicolon_file}")
    
    print(f"\nAll sample files created in: {output_dir.absolute()}")
    print("\nSample files:")
    print("- fasteners_sample.csv: Standard fastener products")
    print("- electronics_sample.csv: Electronic components")
    print("- problematic_sample.csv: Contains validation issues")
    print("- large_sample.csv: 10,000 rows for performance testing")
    print("- semicolon_sample.csv: Uses semicolon delimiter")


if __name__ == "__main__":
    create_sample_files()