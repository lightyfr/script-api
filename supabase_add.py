from supabase_client import supabase

def add_professors_to_supabase(professors):
    inserted = 0
    print(f"[DEBUG] Received {len(professors)} professors to process.")
    # Debug: inspect data structure
    all_keys = set()
    for prof in professors:
        all_keys.update(prof.keys())
    print(f"[DEBUG] Unique keys in professor objects: {all_keys}")
    print(f"[DEBUG] Sample entries: {professors[:5]}")
    
    # Define sets of known junk/placeholder values
    placeholder_names = {"N/A", "Not Found", "Faculty", "Staff", "Visitors", "SC PhD Students", "SE PhD Students"}
    placeholder_emails = {"N/A", "unknown@example.com", "notfound@example.com", "null@example.com", "not_found@example.com", "faculty@example.com", "sc_phd_students@example.com", "se_phd_students@example.com", "staff@example.com", "visitors@example.com", "unknown"}
    
    def is_junk_entry(prof):
        name = (prof.get("name") or "").strip()
        email = (prof.get("email") or "").strip()
        summary = (prof.get("summary") or "").lower()
        # Skip if name or email is a known placeholder
        if name in placeholder_names:
            return True
        if email in placeholder_emails:
            return True
        # Skip if email is not valid
        if "@" not in email or email.startswith("N/A"):
            return True
        # Skip if summary indicates a not found/error page
        if any(x in summary for x in ["not found", "page not found", "content not found"]):
            return True
        return False

    filtered_profs = [prof for prof in professors if not is_junk_entry(prof)]
    print(f"[DEBUG] Filtered out {len(professors) - len(filtered_profs)} junk/placeholder entries.")

    # Remove duplicates within the batch by email (keep the first occurrence)
    seen_emails = set()
    deduped_profs = []
    for prof in filtered_profs:
        email = prof.get("email")
        if email and email not in seen_emails:
            deduped_profs.append(prof)
            seen_emails.add(email)
    print(f"[DEBUG] Deduplicated batch: {len(deduped_profs)} unique emails remain (from {len(filtered_profs)}).")

    emails = [prof.get("email") for prof in deduped_profs if prof.get("email")]
    if not emails:
        print("[DEBUG] No emails found in any entries after filtering; check key names and data from extraction.")
        return inserted
    print(f"[DEBUG] Extracted {len(emails)} emails for checking.")
    # Fetch existing emails in bulk
    existing = supabase.table("scraped_professors").select("email").in_("email", emails).execute()
    print(f"[DEBUG] Supabase select response: data {existing.data}")
    existing_emails = set(row.get("email") for row in (existing.data or []))
    print(f"[DEBUG] Existing emails in DB: {existing_emails}")

    for prof in deduped_profs:
        email = prof.get("email")
        if not email or email in existing_emails:
            continue  # Skip missing or already existing

        research_topics = prof.get("research_topics")
        if isinstance(research_topics, str):
            research_topics = [t.strip() for t in research_topics.split(",") if t.strip()]
        elif not isinstance(research_topics, list):
            research_topics = []

        data = {
            "name": prof.get("name"),
            "email": email,
            "university": prof.get("university"),
            "department": prof.get("department"),
            "research_topics": research_topics,
            "summary": prof.get("summary"),
        }
        print(f"[DEBUG] Preparing upsert for: {data}")
        # Upsert only for entries with valid email
        response = supabase.table("scraped_professors").upsert(data, on_conflict=["email"]).execute()
        print(f"[DEBUG] Upsert response: data {response.data}")
        # If you want to check for errors, inspect response.data or print the whole response object
        if not response.data:
            print(f"Failed to insert {email}: {response}")
        else:
            inserted += 1
    print(f"[DEBUG] Total inserted: {inserted}")
    return inserted