"""
AIF As-At Date Extractor
=========================
Extract the real "as at" date from AIF PDFs using LLM.
"""
import os
import logging
from datetime import datetime, date

log = logging.getLogger(__name__)

def extract_aif_as_at_date(aif_pdf_path: str, company_name: str, 
                          inferred_date: date, model: str = "gpt-4o-mini") -> date | None:
    """
    Extract the real "as at" date from an AIF PDF using LLM.
    Returns the LLM-extracted date if it's within 2 months of inferred_date,
    otherwise returns None (stick with inference).
    
    aif_pdf_path: path to the AIF PDF
    company_name: for context
    inferred_date: our calculated inference for validation
    """
    from openai import OpenAI
    import fitz  # PyMuPDF for PDF text extraction
    
    if not os.path.exists(aif_pdf_path):
        log.warning(f"  AIF PDF not found: {aif_pdf_path}")
        return None
    
    try:
        # Extract first 4-5 pages of text
        with fitz.open(aif_pdf_path) as doc:
            text_parts = []
            for page_num in range(min(5, len(doc))):
                page_text = doc[page_num].get_text()
                text_parts.append(f"=== PAGE {page_num + 1} ===\n{page_text}")
            full_text = "\n\n".join(text_parts)
        
        if len(full_text.strip()) < 100:
            log.warning(f"  AIF text too short ({len(full_text)} chars) - skipping as_at_date extraction")
            return None
        
        # Truncate to ~15k chars to avoid token limits
        if len(full_text) > 15000:
            full_text = full_text[:15000] + "\n[TRUNCATED]"
        
        prompt = f"""You are extracting the "as at" date from an Annual Information Form (AIF) for {company_name}.

The AIF contains information that is current as of a specific date - this is the "as at" date. It's usually mentioned in the first few pages and is typically the last day of a month (e.g., December 31, 2025).

Find this date in the text below. Common phrases that indicate the as at date:
- "as at [date]"
- "as of [date]" 
- "dated [date]"
- "current as at [date]"
- "information contained herein is as at [date]"
- In financial tables or resource statements: "As at December 31, 20XX"

Expected date range: The as at date should be close to {inferred_date.strftime('%B %d, %Y')} (within 1-2 months).

Respond with ONLY the date in YYYY-MM-DD format. If you cannot find a clear as at date, respond with "NOT_FOUND".

AIF TEXT:
{full_text}"""
        
        client = OpenAI()
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=30
        )
        
        result = response.choices[0].message.content.strip()
        log.info(f"  AIF LLM result: {result}")
        
        if result == "NOT_FOUND":
            log.info(f"  AIF: LLM could not find as_at_date - keeping inference {inferred_date}")
            return None
        
        try:
            extracted_date = datetime.strptime(result, "%Y-%m-%d").date()
        except ValueError:
            log.warning(f"  AIF: LLM returned invalid date format '{result}' - keeping inference {inferred_date}")
            return None
        
        # Validate within 2 months of inference
        diff_days = abs((extracted_date - inferred_date).days)
        if diff_days > 62:  # ~2 months
            log.warning(f"  AIF: LLM date {extracted_date} is {diff_days} days from inference {inferred_date} (>2 months) - keeping inference")
            return None
        
        if extracted_date != inferred_date:
            log.info(f"  AIF: correcting as_at_date from {inferred_date} to {extracted_date} ({diff_days} days difference)")
        else:
            log.info(f"  AIF: LLM confirmed as_at_date = {extracted_date}")
        
        return extracted_date
        
    except Exception as e:
        log.error(f"  AIF as_at_date extraction failed: {e}")
        return None