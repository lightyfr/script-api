import asyncio
import json
import os
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


async def find_and_extract_professors(directory_url: str, max_profiles_to_process: int = 10):
    """
    Crawls a faculty directory page to find professor profiles, then crawls each
    profile to extract detailed information using a two-step LLM process.

    Args:
        directory_url: The URL of the main faculty directory page.
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
        # --- STEP 1: Crawl the main directory to get professor profile URLs ---
        print(f"STEP 1: Crawling faculty directory to find professor links at {directory_url}...")

        link_extraction_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            schema=FacultyPage.model_json_schema(),
            instruction="From the provided HTML of a university faculty page, extract the full name and the absolute URL to the profile page for every professor listed. Return a single JSON object with a 'professors' array containing objects with 'name' and 'profile_url' fields. Make sure all URLs are absolute and complete.",
            input_format="html"  # Using HTML is often better for link extraction
        )

        directory_run_config = CrawlerRunConfig(
            extraction_strategy=link_extraction_strategy,
            cache_mode=CacheMode.BYPASS
        )

        directory_result = await crawler.arun(url=directory_url, config=directory_run_config)

        if not directory_result.success or not directory_result.extracted_content:
            print(f"Failed to extract professor links from the directory page. Error: {directory_result.error_message}")
            return

        try:
            # Parse the JSON response which might be an array
            import json
            raw_data = json.loads(directory_result.extracted_content)
            
            # Handle both single object and array responses
            all_professor_links = []
            
            if isinstance(raw_data, list):
                # LLM returned an array of objects
                for item in raw_data:
                    if isinstance(item, dict) and 'professors' in item and not item.get('error', False):
                        # Extract professors from each valid item
                        for prof_data in item['professors']:
                            try:
                                prof_link = ProfessorLink.model_validate(prof_data)
                                all_professor_links.append(prof_link)
                            except Exception as prof_error:
                                print(f"Skipping invalid professor data: {prof_data}. Error: {prof_error}")
            elif isinstance(raw_data, dict):
                # LLM returned a single object matching FacultyPage schema
                faculty_page_data = FacultyPage.model_validate(raw_data)
                all_professor_links = faculty_page_data.professors
            else:
                print("Unexpected LLM response format")
                return
            
            professor_links = all_professor_links
            print(f"Found {len(professor_links)} potential professor profiles.")

            if not professor_links:
                print("No professor profiles found on the directory page. Exiting.")
                return

            # Limit profiles for testing
            if len(professor_links) > max_profiles_to_process:
                print(f"Limiting processing to the first {max_profiles_to_process} profiles.")
                professor_links = professor_links[:max_profiles_to_process]
            
            profile_urls = [str(prof.profile_url) for prof in professor_links]

        except Exception as e:
            print(f"Error parsing professor links from LLM response: {e}")
            print("Raw LLM output:", directory_result.extracted_content)
            return

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
                    
                    # Skip if email is missing, as per your original logic
                    if not professor_data.email:
                        print(f"  - Skipping {professor_data.name} (no email found).")
                        continue

                    all_professor_details.append(professor_data)
                    print(f"  Successfully extracted details for: {professor_data.name}")

                except Exception as e:
                    print(f"  - Could not parse or validate data for {result.url}. Error: {e}")
            else:
                print(f"  - Failed to process or extract from {result.url}. Error: {result.error_message}")

    # --- FINAL OUTPUT and DATABASE INSERT ---
    print("\n--- Completed Extraction ---")
    print(f"Successfully extracted details for {len(all_professor_details)} professors.")

    if all_professor_details:
        # Convert to list of dicts for Supabase function
        output_data = [prof.model_dump() for prof in all_professor_details]
        
        # Save to a local JSON file for inspection
        output_filename = "professors_data.json"
        with open(output_filename, "w") as f:
            json.dump(output_data, f, indent=4)
        print(f"\nFull data saved to {output_filename}")

        # Add to Supabase
        print("\nAdding records to Supabase...")
        inserted_count = add_professors_to_supabase(output_data)
        print(f"Attempted to save {len(output_data)} professors; actually inserted {inserted_count} new records to Supabase.")
    else:
        print("No valid professor data was collected to save.")


if __name__ == "__main__":
    # URL of the main faculty listing page
    FACULTY_DIRECTORY_URL = "https://s3d.cmu.edu/people/faculty-index.html"
    
    # To control costs and time during testing, we'll only process the first 5 profiles found.
    # Change this number to process more.
    MAX_PROFILES_TO_PROCESS = 50000

    asyncio.run(find_and_extract_professors(FACULTY_DIRECTORY_URL, max_profiles_to_process=MAX_PROFILES_TO_PROCESS))