"""Focused tests for US/EU scraper detail enrichment."""

from types import SimpleNamespace

from src.scrapers.academicpositions import AcademicPositionsScraper
from src.scrapers.euraxess import EuraxessScraper
from src.scrapers.institutional import InstitutionalPortalScraper
from src.scrapers.jobspy_scraper import JobSpyScraper
from src.scrapers.jobs_ac_uk import JobsAcUkScraper


def test_jobs_ac_uk_detail_enrichment_extracts_metadata():
    scraper = JobsAcUkScraper()
    scraper._fetch_with_browser = lambda url: """
        <html><body>
          <div id="job-description">
            <p>About the Role</p>
            <p>Lead spatial transcriptomics analyses.</p>
            <p>About You</p>
            <p>PhD in computational biology.</p>
            <p>How To Apply</p>
            <p>Please submit a CV, cover letter, and publication list. Contact jobs@example.ac.uk</p>
          </div>
          <div class="j-advert-details__first-col">
            <div>Location:</div><div>Oxford, United Kingdom</div>
            <div>Salary:</div><div>£39,424 to £47,779 per annum</div>
            <div>Hours:</div><div>Full Time</div>
            <div>Contract Type:</div><div>Fixed-Term/Contract</div>
          </div>
          <div class="j-advert-details__second-col">
            <div>Placed On:</div><div>25th February 2026</div>
            <div>Closes:</div><div>16th March 2026</div>
          </div>
        </body></html>
    """

    enriched = scraper._enrich_from_detail({"url": "https://example.com/job/DQQ699"})

    assert enriched["posted_date"] == "2026-02-25"
    assert enriched["deadline"] == "2026-03-16"
    assert "Salary: £39,424 to £47,779 per annum" in enriched["conditions"]
    assert "Hours: Full Time" in enriched["conditions"]
    assert "Contract Type: Fixed-Term/Contract" in enriched["conditions"]
    assert enriched["application_materials"] == "CV; Cover letter; Publication list"
    assert enriched["contact_email"] == "jobs@example.ac.uk"


def test_euraxess_detail_enrichment_promotes_offer_metadata():
    scraper = EuraxessScraper()
    scraper.fetch = lambda url, **kwargs: SimpleNamespace(
        status_code=200,
        text="""
            <html><body>
              <section>
                <h2 id="job-information">Job information</h2>
                <dl class="ecl-description-list">
                  <dt class="ecl-description-list__term">Organisation/Company</dt>
                  <dd class="ecl-description-list__definition">University of Example</dd>
                  <dt class="ecl-description-list__term">Research Field</dt>
                  <dd class="ecl-description-list__definition">Biological sciences » Spatial biology</dd>
                  <dt class="ecl-description-list__term">Country</dt>
                  <dd class="ecl-description-list__definition">France</dd>
                  <dt class="ecl-description-list__term">Type of Contract</dt>
                  <dd class="ecl-description-list__definition">Temporary</dd>
                  <dt class="ecl-description-list__term">Hours Per Week</dt>
                  <dd class="ecl-description-list__definition">40</dd>
                  <dt class="ecl-description-list__term">Offer Starting Date</dt>
                  <dd class="ecl-description-list__definition"><time datetime="2026-05-01">1 May 2026</time></dd>
                </dl>
              </section>
              <section>
                <h2 id="offer-description">Offer description</h2>
                <div>
                  Locations:
                  France, Lille
                  Time type:
                  Full time
                  Department:
                  Spatial Biology
                  Posting End Date:
                  March 26, 2026
                  Job End Date:
                  March 31, 2028
                  The expected pay range for this position is $7,000 - $7,350/month.
                  Please submit a CV and publication list. Contact pi@example.edu
                </div>
              </section>
              <section>
                <h2 id="additional-information">Additional information</h2>
                <div>
                  <div class="ecl-u-type-bold">Benefits</div>
                  <div>Relocation support and pension.</div>
                </div>
              </section>
            </body></html>
        """,
    )

    enriched = scraper._enrich_from_detail({"url": "https://euraxess.ec.europa.eu/jobs/412754"})

    assert enriched["institute"] == "University of Example"
    assert enriched["department"] == "Spatial Biology"
    assert enriched["country"] == "France"
    assert enriched["deadline"] == "2026-03-26"
    assert "Type of Contract: Temporary" in enriched["conditions"]
    assert "Hours Per Week: 40" in enriched["conditions"]
    assert "Start: 2026-05-01" in enriched["conditions"]
    assert "Duration: until 2028-03-31" in enriched["conditions"]
    assert "Salary: $7,000 - $7,350/month" in enriched["conditions"]
    assert enriched["application_materials"] == "CV; Publication list"
    assert enriched["contact_email"] == "pi@example.edu"


def test_institutional_mpg_detail_enrichment_extracts_labeled_blocks():
    scraper = InstitutionalPortalScraper()
    scraper.fetch = lambda url, **kwargs: SimpleNamespace(
        text="""
            <html><body>
              <main>
                <p>Application Round</p>
                <p>2026-A</p>
                <p>Project title</p>
                <p>Bridge RNA regulation in bacteria</p>
                <p>City</p>
                <p>Martinsried</p>
                <p>Specific field of research</p>
                <p>RNA Biology</p>
                <p>Max Planck Institute</p>
                <p>Max Planck Institute of Biochemistry</p>
                <p>Qualifications</p>
                <p>PhD in molecular biology</p>
                <p>Requirements</p>
                <p>Experience with live-cell imaging</p>
                <p>Additional requirements for the application</p>
                <p>Please submit a CV, cover letter, and publication list</p>
                <p>Principal Investigator</p>
                <p>Prof. Jane Doe</p>
                <p>Email</p>
                <p>jane.doe@example.mpg.de</p>
              </main>
            </body></html>
        """,
    )

    enriched = scraper._enrich_mpg_detail(
        {"url": "https://postdocprogram.mpg.de/node/38398", "institute": "Max Planck Society"}
    )

    assert enriched["field"] == "RNA Biology"
    assert enriched["institute"] == "Max Planck Institute of Biochemistry"
    assert "City: Martinsried" in enriched["conditions"]
    assert "Qualifications: PhD in molecular biology" in enriched["requirements"]
    assert "Requirements: Experience with live-cell imaging" in enriched["requirements"]
    assert "Jane Doe" in enriched["pi_name"]
    assert enriched["contact_email"] == "jane.doe@example.mpg.de"
    assert enriched["application_materials"] == "CV; Cover letter; Publication list"


def test_academicpositions_detail_enrichment_promotes_metadata_rows():
    scraper = AcademicPositionsScraper()
    scraper.fetch = lambda url, **kwargs: SimpleNamespace(
        text="""
            <html><body>
              <div class="row mb-3">
                <div class="col-12 col-md-4"><div class="font-weight-bold">Published</div></div>
                <div class="col-auto col-md-8">March 1, 2026</div>
              </div>
              <div class="row mb-3">
                <div class="col-12 col-md-4"><div class="font-weight-bold">Application deadline</div></div>
                <div class="col-auto col-md-8">March 16, 2026</div>
              </div>
              <div class="row mb-3">
                <div class="col-12 col-md-4"><div class="font-weight-bold">Job type</div></div>
                <div class="col-auto col-md-8">Full-time, Fixed-term</div>
              </div>
              <div class="row mb-3">
                <div class="col-12 col-md-4"><div class="font-weight-bold">Start date</div></div>
                <div class="col-auto col-md-8">June 1, 2026</div>
              </div>
              <div class="row mb-3">
                <div class="col-12 col-md-4"><div class="font-weight-bold">Field</div></div>
                <div class="col-auto col-md-8">Virology,,Drug discovery</div>
              </div>
              <div class="editor ck-content">
                <p>The Antiviral Drug Discovery Unit is hiring a postdoc.</p>
                <p>Please submit a CV, cover letter, and publication list. Contact pi@example.se</p>
              </div>
            </body></html>
        """,
    )

    enriched = scraper._enrich_from_detail(
        {"url": "https://academicpositions.com/ad/example/2026/postdoc/123456"}
    )

    assert enriched["posted_date"] == "2026-03-01"
    assert enriched["deadline"] == "2026-03-16"
    assert "Job type: Full-time, Fixed-term" in enriched["conditions"]
    assert "Start: June 1, 2026" in enriched["conditions"]
    assert enriched["field"] == "Virology, Drug discovery"
    assert enriched["application_materials"] == "CV; Cover letter; Publication list"
    assert enriched["contact_email"] == "pi@example.se"


def test_jobspy_linkedin_detail_enrichment_extracts_guest_metadata():
    scraper = JobSpyScraper()
    scraper._fetch_linkedin_html = lambda url: """
        <html><body>
          <a class="topcard__org-name-link">Genentech</a>
          <span class="posted-time-ago__text">1 week ago</span>
          <span class="topcard__flavor topcard__flavor--bullet">South San Francisco, CA, United States</span>
          <div class="show-more-less-html__markup">
            About the Role
            Lead translational medicine studies.
            How To Apply
            Please submit a CV and cover letter. Contact lab@genentech.org
          </div>
          <ul>
            <li class="description__job-criteria-item">
              <h3>Employment type</h3><span>Full-time</span>
            </li>
            <li class="description__job-criteria-item">
              <h3>Industries</h3><span>Biotechnology Research</span>
            </li>
          </ul>
        </body></html>
    """

    enriched = scraper._enrich_linkedin_detail(
        {"url": "https://www.linkedin.com/jobs/view/4362963648", "source": "jobspy_linkedin"}
    )

    assert enriched["institute"] == "Genentech"
    assert enriched["country"] == "United States"
    assert enriched["posted_date"]
    assert "Employment type: Full-time" in enriched["conditions"]
    assert enriched["application_materials"] == "CV; Cover letter"
    assert enriched["contact_email"] == "lab@genentech.org"
