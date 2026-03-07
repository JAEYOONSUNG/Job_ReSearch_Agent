"""Tests for Korean detail extraction and Excel-oriented field filling."""

from bs4 import BeautifulSoup

from src.matching.job_parser import parse_structured_description
from src.scrapers.korean_jobs import (
    KoreanJobsScraper,
    _apply_korean_fallbacks,
    _extract_korean_fields,
    _is_bio_relevant_korean,
    _parse_institute_from_title,
)


def test_extract_korean_fields_builds_structured_description():
    html = """
    <html>
      <body>
        <table>
          <tr><th>모집기관</th><td>한국생명공학연구원</td></tr>
          <tr><th>소속 부서</th><td>합성생물학연구센터</td></tr>
          <tr><th>연구분야</th><td>합성생물학 / RNA biology</td></tr>
          <tr><th>연구책임자</th><td>김철수 교수 (kim@example.org)</td></tr>
          <tr><th>모집내용</th><td>Bridge RNA 기반 차세대 유전자 조절 기술 개발</td></tr>
          <tr><th>담당업무</th><td>CRISPR assay 개발 및 mammalian cell screening</td></tr>
          <tr><th>지원자격</th><td>생명과학 관련 박사학위, 분자생물학 경험</td></tr>
          <tr><th>우대사항</th><td>NGS 분석 및 bioinformatics 경험</td></tr>
          <tr><th>제출서류</th><td>이력서, 자기소개서, 연구계획서, 추천서, 학위증명서</td></tr>
          <tr><th>급여</th><td>연 6,000만원</td></tr>
          <tr><th>계약기간</th><td>2년</td></tr>
          <tr><th>근무지역</th><td>대전</td></tr>
          <tr><th>공고일</th><td>2026.03.05</td></tr>
          <tr><th>모집기간</th><td>2026.03.01 ~ 2026.03.31</td></tr>
          <tr><th>문의</th><td>kim@example.org</td></tr>
        </table>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    job = {
        "title": "한국생명공학연구원 박사후연구원 모집",
        "description": "세포기반 유전자 조절 플랫폼 연구를 수행합니다.",
    }

    _extract_korean_fields(soup, job)

    assert job["institute"] == "한국생명공학연구원"
    assert job["department"] == "합성생물학연구센터"
    assert job["field"] == "합성생물학 / RNA biology"
    assert job["pi_name"] == "김철수 교수"
    assert job["posted_date"] == "2026-03-05"
    assert job["deadline"] == "2026-03-31"
    assert job["contact_email"] == "kim@example.org"
    assert "박사학위" in job["requirements"]
    assert "연구계획서" in job["application_materials"]
    assert "급여:" in job["conditions"]
    assert "근무지:" in job["conditions"]

    sections = parse_structured_description(job["description"])
    assert "Bridge RNA" in sections["summary"]
    assert "CRISPR assay" in sections["responsibilities"]
    assert "분자생물학" in sections["requirements"]
    assert "bioinformatics" in sections["preferred"]
    assert "대전" in sections["conditions"]


def test_extract_korean_fields_supports_div_head_detail_layout():
    html = """
    <html>
      <body>
        <div class="b-view-detail-box">
          <li><div class="b-head">소속책임자</div><div class="b-detail">김철수</div></li>
          <li><div class="b-head">채용 웹페이지</div><div class="b-detail url"><a href="https://example.org/apply">https://example.org/apply</a></div></li>
        </div>
        <div class="b-view-recruit-info-box">
          <div class="recruit-info"><div class="b-head">채용방식</div><div class="b-detail">정규직</div></div>
          <div class="recruit-info"><div class="b-head">급여조건</div><div class="b-detail">연봉 5000만원 이상</div></div>
          <div class="recruit-info"><div class="b-head">최종학력</div><div class="b-detail">박사</div></div>
        </div>
        <div class="b-view-recruit-work-box">
          <div class="b-head">담당업무1</div>
          <div class="b-detail"><pre class="b-pre">Bridge RNA assay 개발</pre></div>
        </div>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    job = {"title": "한국생명공학연구원 박사후연구원 채용"}

    _extract_korean_fields(soup, job)

    assert job["pi_name"] == "김철수"
    assert "정규직" in job["conditions"]
    assert "연봉 5000만원 이상" in job["conditions"]
    assert "담당 업무" in job["description"]
    assert "Bridge RNA assay 개발" in job["description"]
    assert "https://example.org/apply" in job["description"]


def test_parse_generic_row_extracts_hibrain_listing_dates():
    html = """
    <li class="row sortRoot">
      <span class="td_title listTypeRecruit">
        <a href="/recruitment/recruits/3568205?page=2" title="서울대학교 종합약학연구소 연구교원 모집 (5차)">
          <span class="title titleImageNone">서울대학교 종합약학연구소 연구교원 모집 (5차)</span>
        </a>
      </span>
      <span class="infoBlock">
        <span class="td_receipt">
          <span class="number">26.03.05</span>
          <span class="specialCharacter">~</span>
          <span class="number">26.03.13</span>
        </span>
        <span class="td_rdtm number">26.03.06</span>
      </span>
    </li>
    """
    row = BeautifulSoup(html, "html.parser").select_one("li")
    scraper = KoreanJobsScraper()

    job = scraper._parse_generic_row(row, "https://www.hibrain.net", "hibrain")

    assert job is not None
    assert job["title"] == "서울대학교 종합약학연구소 연구교원 모집 (5차)"
    assert job["posted_date"] == "2026-03-05"
    assert job["deadline"] == "2026-03-13"
    assert job["institute"] == "서울대학교"


def test_parse_institute_from_title_handles_hibrain_variants():
    inst, dept = _parse_institute_from_title(
        "경북대학교 의과대학 면역독성/약리학교실 Post-Doc. 모집"
    )
    assert inst == "경북대학교"
    assert dept == "의과대학 면역독성/약리학교실"

    inst2, dept2 = _parse_institute_from_title(
        "UNIST 생명과학과 Post-Doc. 모집"
    )
    assert inst2 == "Ulsan National Institute of Science and Technology (UNIST)"
    assert dept2 == "생명과학과"


def test_hibrain_fallback_description_preserves_listing_metadata():
    job = {
        "title": "UNIST 생명과학과 Post-Doc. 모집",
        "source": "korean_jobs:hibrain",
        "posted_date": "2026-03-05",
        "deadline": "2026-03-13",
    }

    _apply_korean_fallbacks(job)

    assert "모집 내용" in job["description"]
    assert "UNIST" in job["description"]
    assert "마감일: 2026-03-13" in job["description"]


def test_is_bio_relevant_korean_filters_false_positive_company_titles():
    assert not _is_bio_relevant_korean("한화생명 인천검진센터 센터장 모집")
    assert not _is_bio_relevant_korean("전라북도 남원의료원 영상의학과 의사 모집")
    assert _is_bio_relevant_korean("UNIST 생명과학과 Post-Doc. 모집")
