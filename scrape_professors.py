import asyncio
import json
import os
import re
from typing import List, Optional

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    LLMConfig,
    LLMExtractionStrategy,
    CacheMode
)
from dotenv import load_dotenv
from pydantic import BaseModel, Field, HttpUrl
from supabase_add import add_professors_to_supabase

# Load environment variables from .env file
load_dotenv()

# --- Validation Functions ---
def is_valid_email(email: str) -> bool:
    """
    Validate if an email address is properly formatted and looks legitimate.
    """
    if not email or not isinstance(email, str):
        return False
    
    # Basic email regex pattern
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Check basic format
    if not re.match(email_pattern, email):
        return False
    
    # Additional checks for suspicious patterns
    email = email.lower()
    
    # Reject common fake/placeholder emails
    fake_patterns = [
        'example.com', 'test.com', 'fake.com', 'dummy.com',
        'placeholder.com', 'sample.com', 'lorem.ipsum'
    ]
    
    if any(pattern in email for pattern in fake_patterns):
        return False
    
    # Reject emails that are just random characters
    local_part = email.split('@')[0]
    if len(local_part) < 2 or local_part.isdigit():
        return False
    
    return True

def is_valid_name(name: str) -> bool:
    """
    Validate if a name looks like a real person's name.
    """
    if not name or not isinstance(name, str):
        return False
    
    name = name.strip()
    
    # Name should have at least 2 characters
    if len(name) < 2:
        return False
    
    # Name should contain at least one letter
    if not re.search(r'[a-zA-Z]', name):
        return False
    
    # Split into parts to check structure
    name_parts = name.split()
    
    # Should have at least first and last name (2 parts)
    if len(name_parts) < 2:
        return False
    
    # Each part should be reasonable length and contain letters
    for part in name_parts:
        if len(part) < 1 or not re.search(r'[a-zA-Z]', part):
            return False
        
        # Reject parts that are all numbers or symbols
        if part.isdigit() or not re.search(r'[a-zA-Z]', part):
            return False
    
    # Reject obvious test/fake names
    fake_name_patterns = [
        'test', 'example', 'sample', 'dummy', 'fake', 'lorem', 'ipsum',
        'john doe', 'jane doe', 'firstname lastname', 'unknown', 'n/a', 
        'anonymous', 'visitor', 'staff', 'faculty', 'sc phd students',
        'se phd students', 'professor', 'instructor', 'lecturer', 'researcher'
    ]
    
    name_lower = name.lower()
    if any(pattern in name_lower for pattern in fake_name_patterns):
        return False
    
    return True

# --- Define the Schemas for Data Extraction using Pydantic ---

# Final, detailed schema for an individual professor's page
class ProfessorSchema(BaseModel):
    name: str = Field(description="The full name of the professor.")
    email: Optional[str] = Field(None, description="The professor's email address.")
    university: Optional[str] = Field(None, description="The university they are affiliated with (e.g., 'Carnegie Mellon University').")
    department: Optional[str] = Field(None, description="The specific department of the professor (e.g., 'Software and Societal Systems').")
    research_topics: Optional[List[str]] = Field(
        None, description="A list of the professor's primary research topics or interests."
    )
    summary: Optional[str] = Field(
        None, description="A brief one or two-sentence summary of the professor's work or role."
    )

# A temporary schema to find links on the main faculty directory page
class ProfessorLink(BaseModel):
    name: str = Field(description="The professor's full name as listed in the directory.")
    profile_url: HttpUrl = Field(description="The full, absolute URL to the professor's individual profile page.")

class FacultyPage(BaseModel):
    professors: List[ProfessorLink]


def validate_professor_data(professor: ProfessorSchema) -> bool:
    """
    Validate that a professor's data meets quality standards before insertion.
    """
    # Validate name
    if not is_valid_name(professor.name):
        print(f"  - Invalid name: '{professor.name}'")
        return False
    
    # Validate email if present
    if professor.email and not is_valid_email(professor.email):
        print(f"  - Invalid email for {professor.name}: '{professor.email}'")
        return False
    
    # Require email to be present for insertion
    if not professor.email:
        print(f"  - No email provided for {professor.name}")
        return False
    
    return True


async def find_and_extract_professors(directory_urls: List[str], max_profiles_to_process: int = 10):
    """
    Crawls multiple faculty directory pages to find professor profiles, then crawls each
    profile to extract detailed information using a two-step LLM process.

    Args:
        directory_urls: A list of URLs of faculty directory pages to crawl.
        max_profiles_to_process: Limit the number of profiles to process to control costs during testing.
    """
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    print("Using Gemini API Key:", gemini_api_key)
    if not gemini_api_key:
        print("FATAL: GEMINI_API_KEY not found in environment. Please set it in your .env file or environment.")
        return

    # --- Configure the LLM and Crawler ---
    llm_config = LLMConfig(
        provider="gemini/gemini-2.0-flash", # A powerful and cost-effective model
        api_token=gemini_api_key,
    )
    browser_config = BrowserConfig(headless=True)
    all_professor_details: List[ProfessorSchema] = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Process each directory URL
        for directory_url in directory_urls:
            print(f"\n=== Processing directory: {directory_url} ===")
            
            # --- STEP 1: Crawl the main directory to get professor profile URLs ---
            print(f"STEP 1: Crawling faculty directory to find professor links at {directory_url}...")

            link_extraction_strategy = LLMExtractionStrategy(
                llm_config=llm_config,
                schema=FacultyPage.model_json_schema(),
                instruction="From the provided HTML of a university faculty page, extract the full name and the absolute URL to **ALL** the profile pages for every professor listed. Return a single JSON object with a 'professors' array containing objects with 'name' and 'profile_url' fields. Make sure all URLs are absolute and complete.",
                input_format="html"  # Using HTML is often better for link extraction
            )

            directory_run_config = CrawlerRunConfig(
                extraction_strategy=link_extraction_strategy,
                cache_mode=CacheMode.BYPASS
            )

            directory_result = await crawler.arun(url=directory_url, config=directory_run_config)

            if not directory_result.success or not directory_result.extracted_content:
                print(f"Failed to extract professor links from the directory page. Error: {directory_result.error_message}")
                continue  # Skip to next URL

            try:
                # Parse the JSON response which might be an array
                import json
                raw_data = json.loads(directory_result.extracted_content)
                
                # Handle both single object and array responses
                current_professor_links = []
            
                if isinstance(raw_data, list):
                    # LLM returned an array of objects
                    for item in raw_data:
                        if isinstance(item, dict) and 'professors' in item and not item.get('error', False):
                            # Extract professors from each valid item
                            for prof_data in item['professors']:
                                try:
                                    prof_link = ProfessorLink.model_validate(prof_data)
                                    current_professor_links.append(prof_link)
                                except Exception as prof_error:
                                    print(f"Skipping invalid professor data: {prof_data}. Error: {prof_error}")
                elif isinstance(raw_data, dict):
                    # LLM returned a single object matching FacultyPage schema
                    faculty_page_data = FacultyPage.model_validate(raw_data)
                    current_professor_links = faculty_page_data.professors
                else:
                    print("Unexpected LLM response format")
                    continue  # Skip to next URL
            
                print(f"Found {len(current_professor_links)} potential professor profiles from {directory_url}.")

                if not current_professor_links:
                    print("No professor profiles found on this directory page. Continuing to next URL.")
                    continue  # Skip to next URL

                # Limit profiles for testing
                if len(current_professor_links) > max_profiles_to_process:
                    print(f"Limiting processing to the first {max_profiles_to_process} profiles from this directory.")
                    current_professor_links = current_professor_links[:max_profiles_to_process]
            
                profile_urls = [str(prof.profile_url) for prof in current_professor_links]

            except Exception as e:
                print(f"Error parsing professor links from LLM response: {e}")
                print("Raw LLM output:", directory_result.extracted_content)
                continue  # Skip to next URL

            # --- STEP 2: Crawl individual professor pages to extract details ---
            print(f"\nSTEP 2: Crawling {len(profile_urls)} individual profile pages for detailed extraction...")

            details_extraction_strategy = LLMExtractionStrategy(
                llm_config=llm_config,
                schema=ProfessorSchema.model_json_schema(),
                instruction="From the professor's profile page, extract their full name, email, university, department, research topics, and a brief summary. If a field is not present, omit it from the JSON.",
                input_format="html"  # Markdown is usually cleaner for text extraction
            )

            details_run_config = CrawlerRunConfig(
                extraction_strategy=details_extraction_strategy,
                word_count_threshold=50,  # Ensure page has some content
                cache_mode=CacheMode.ENABLED  # Enable cache for repeated runs on same profiles
            )

            results_container = await crawler.arun_many(urls=profile_urls, config=details_run_config)

            for result in results_container:
                if result.success and result.extracted_content:
                    try:
                        # The LLM might return a list with one item, so we handle both cases
                        extracted_list = json.loads(result.extracted_content)
                        if isinstance(extracted_list, list) and extracted_list:
                            professor_data = ProfessorSchema.model_validate(extracted_list[0])
                        elif isinstance(extracted_list, dict):
                            professor_data = ProfessorSchema.model_validate(extracted_list)
                        else:
                            continue
                        
                        # Validate professor data before adding
                        if not validate_professor_data(professor_data):
                            print(f"  - Skipping {professor_data.name} (failed validation).")
                            continue

                        all_professor_details.append(professor_data)
                        print(f"  Successfully extracted and validated details for: {professor_data.name}")

                    except Exception as e:
                        print(f"  - Could not parse or validate data for {result.url}. Error: {e}")
                else:
                    print(f"  - Failed to process or extract from {result.url}. Error: {result.error_message}")

    # --- FINAL OUTPUT and DATABASE INSERT ---
    print("\n--- Completed Extraction ---")
    print(f"Successfully extracted details for {len(all_professor_details)} professors.")

    if all_professor_details:
        # Final validation pass before database insertion
        valid_professors = []
        print("\n--- Final Validation Pass ---")
        for prof in all_professor_details:
            if validate_professor_data(prof):
                valid_professors.append(prof)
            else:
                print(f"  - Excluded {prof.name} from database insertion (failed final validation)")
        
        print(f"After validation: {len(valid_professors)} out of {len(all_professor_details)} professors are valid for insertion.")
        
        if not valid_professors:
            print("No valid professor data passed final validation.")
            return
        
        # Convert to list of dicts for Supabase function
        output_data = [prof.model_dump() for prof in valid_professors]
        
        # Save to a local JSON file for inspection
        output_filename = "professors_data.json"
        with open(output_filename, "w") as f:
            json.dump(output_data, f, indent=4)
        print(f"\nValidated data saved to {output_filename}")

        # Add to Supabase
        print("\nAdding validated records to Supabase...")
        inserted_count = add_professors_to_supabase(output_data)
        print(f"Attempted to save {len(output_data)} validated professors; actually inserted {inserted_count} new records to Supabase.")
    else:
        print("No valid professor data was collected to save.")


if __name__ == "__main__":
    # List of faculty directory URLs to crawl
    FACULTY_DIRECTORY_URLS = [
        "https://science.gmu.edu/academics/departments-units/mathematical-sciences/faculty-and-staff",
        "https://ids.vcu.edu/about-us/our-faculty--staff/",
        "https://www.scs.gatech.edu/people/faculty",
        "https://geog.umd.edu/people/professors",
        "https://statistics.northwestern.edu/people/faculty/",
        "https://be.mit.edu/faculty/",
        "https://imes.mit.edu/people/faculty/",
        "https://cee.mit.edu/faculty/",
        "https://www.eecs.mit.edu/role/faculty/?fwp_role=faculty&fwp_research=robotics",
        "https://web.mit.edu/nse/people/faculty/",
        "https://web.cs.dartmouth.edu/people",
        "https://www.hbs.edu/faculty/Pages/browse.aspx",
        "https://drb.hms.harvard.edu/faculty-alphabetical-order",
        "https://www2.eecs.berkeley.edu/Faculty/Lists/faculty.html",
        "https://www.sps.nyu.edu/homepage/academics/faculty-directory.html",
        "https://liberalstudies.nyu.edu/about/faculty-listing.html",
        "https://www.anderson.ucla.edu/faculty-and-research/faculty-directory",
        "https://umdearborn.edu/cecs/about/faculty-directory",
        "https://ati.osu.edu/aboutus/directory",
        

        

    ]
    
    # To control costs and time during testing, we'll only process the first profiles found.
    # Change this number to process more.
    MAX_PROFILES_TO_PROCESS = 500000000

    asyncio.run(find_and_extract_professors(FACULTY_DIRECTORY_URLS, max_profiles_to_process=MAX_PROFILES_TO_PROCESS))