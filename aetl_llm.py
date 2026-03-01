"""
================================================================================
AETL LLM Provider  —  통합 LLM 초기화 유틸리티
================================================================================
LLM_PROVIDER 환경변수로 사용할 LLM을 선택할 수 있습니다.

  - gemini  : Google Gemini 2.5 Flash
  - claude  : Anthropic Claude Sonnet
  - openai  : OpenAI GPT-4o-mini
  - (미설정) : 사용 가능한 API 키를 순서대로 시도 (gemini → claude → openai)

설정 예시 (.env):
  LLM_PROVIDER=claude
================================================================================
"""

import os

from dotenv import load_dotenv

load_dotenv()


def _try_gemini():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return None
    from langchain_google_genai import ChatGoogleGenerativeAI
    return ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=api_key,
        temperature=0.0,
    )


def _try_claude():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    from langchain_anthropic import ChatAnthropic
    return ChatAnthropic(
        model="claude-sonnet-4-6",
        temperature=0.0,
        api_key=api_key,
    )


def _try_openai():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model="gpt-4o-mini",
        temperature=0.0,
        api_key=api_key,
    )


_PROVIDERS = {
    "gemini": _try_gemini,
    "claude": _try_claude,
    "openai": _try_openai,
}

_DEFAULT_ORDER = [_try_gemini, _try_claude, _try_openai]


def get_llm(with_tools: list | None = None):
    """
    LLM 인스턴스를 반환합니다.

    LLM_PROVIDER 환경변수가 설정되면 해당 프로바이더를 **우선** 사용하고,
    런타임 오류(크레딧 부족 등) 발생 시 다른 프로바이더로 자동 fallback합니다.
    LLM_PROVIDER가 미설정이면 사용 가능한 키를 순서대로 시도합니다.

    Args:
        with_tools: Tool 리스트 (Agent용 bind_tools가 필요한 경우)

    Returns:
        LLM 인스턴스 (with_tools가 있으면 tool-bound LLM, fallback 체인 포함)
    """
    provider = os.getenv("LLM_PROVIDER", "").lower().strip()

    # ── LLM_PROVIDER가 명시적으로 지정된 경우: 우선 시도 + fallback 체인 ──
    if provider:
        if provider not in _PROVIDERS:
            raise ValueError(
                f"알 수 없는 LLM_PROVIDER 값입니다: '{provider}'\n"
                f"지원 값: {', '.join(_PROVIDERS.keys())}"
            )
        # 지정된 provider를 primary로, 나머지를 fallback 순서로 배치
        primary_fn = _PROVIDERS[provider]
        fallback_fns = [fn for name, fn in _PROVIDERS.items() if name != provider]

        primary = None
        try:
            primary = primary_fn()
        except Exception:
            pass

        if primary is None:
            env_key = {
                "gemini": "GOOGLE_API_KEY",
                "claude": "ANTHROPIC_API_KEY",
                "openai": "OPENAI_API_KEY",
            }.get(provider, "API_KEY")
            # primary 초기화 실패 → fallback 시도
            for fb_fn in fallback_fns:
                try:
                    fb = fb_fn()
                    if fb is not None:
                        if with_tools:
                            return fb.bind_tools(with_tools)
                        return fb
                except Exception:
                    continue
            raise RuntimeError(
                f"LLM_PROVIDER={provider} 로 설정되었지만 "
                f"{env_key} 환경변수가 비어있고, fallback 프로바이더도 실패했습니다."
            )

        # primary 성공 → fallback 체인 구성 (런타임 오류 대비)
        fallbacks = []
        for fb_fn in fallback_fns:
            try:
                fb = fb_fn()
                if fb is not None:
                    fallbacks.append(fb)
            except Exception:
                continue

        if fallbacks:
            llm = primary.with_fallbacks(fallbacks)
        else:
            llm = primary

        if with_tools:
            # with_fallbacks LLM은 bind_tools를 직접 지원하지 않으므로
            # 각 LLM에 개별 bind → fallback 재구성
            primary_bound = primary.bind_tools(with_tools)
            fallback_bound = []
            for fb in fallbacks:
                try:
                    fallback_bound.append(fb.bind_tools(with_tools))
                except Exception:
                    continue
            if fallback_bound:
                return primary_bound.with_fallbacks(fallback_bound)
            return primary_bound
        return llm

    # ── LLM_PROVIDER 미설정: 사용 가능한 프로바이더를 순서대로 시도 ──
    available = []
    errors = []
    for try_fn in _DEFAULT_ORDER:
        try:
            llm = try_fn()
            if llm is not None:
                available.append(llm)
        except Exception as e:
            errors.append(f"{try_fn.__name__}: {e}")

    if not available:
        raise RuntimeError(
            "사용 가능한 LLM API 키가 없습니다.\n"
            ".env 파일에 다음 중 하나를 설정하세요:\n"
            "  GOOGLE_API_KEY    (Gemini)\n"
            "  ANTHROPIC_API_KEY (Claude)\n"
            "  OPENAI_API_KEY    (OpenAI)\n"
            "선택적으로 LLM_PROVIDER=gemini|claude|openai 로 프로바이더를 지정할 수 있습니다.\n"
            + (f"오류: {'; '.join(errors)}" if errors else "")
        )

    primary = available[0]
    fallbacks = available[1:]

    if with_tools:
        primary_bound = primary.bind_tools(with_tools)
        fallback_bound = []
        for fb in fallbacks:
            try:
                fallback_bound.append(fb.bind_tools(with_tools))
            except Exception:
                continue
        if fallback_bound:
            return primary_bound.with_fallbacks(fallback_bound)
        return primary_bound

    if fallbacks:
        return primary.with_fallbacks(fallbacks)
    return primary


def call_llm(prompt: str) -> str:
    """
    프롬프트를 LLM에 전달하고 텍스트 응답을 반환합니다.
    _call_llm() 중복 코드를 대체하는 공용 함수입니다.
    """
    try:
        llm = get_llm()
        response = llm.invoke(prompt)
        return response.content if hasattr(response, "content") else str(response)
    except Exception as e:
        return f'{{"error": "LLM 호출 실패: {e}"}}'


# ── PDF 네이티브 지원 프로바이더 (문서 전체를 LLM에 직접 전달) ──
_PDF_PROVIDERS = ["gemini", "claude"]  # OpenAI gpt-4o-mini는 PDF 미지원


def call_llm_with_pdf(prompt: str, pdf_bytes: bytes) -> str:
    """
    PDF 문서 원본을 LLM에 직접 전달하여 분석합니다.
    Claude(document 블록)와 Gemini(inline data) 네이티브 PDF 분석을 지원합니다.
    모든 프로바이더 실패 시 텍스트 추출 fallback을 시도합니다.

    Args:
        prompt: PDF에 대한 분석 요청 프롬프트
        pdf_bytes: PDF 파일 바이트

    Returns:
        LLM 응답 텍스트
    """
    import base64
    from langchain_core.messages import HumanMessage

    b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    # provider 우선순위 결정
    provider = os.getenv("LLM_PROVIDER", "").lower().strip()
    order = list(_PDF_PROVIDERS)
    if provider in _PDF_PROVIDERS:
        order = [provider] + [p for p in order if p != provider]

    errors = []
    for prov_name in order:
        try:
            llm_fn = _PROVIDERS.get(prov_name)
            if not llm_fn:
                continue
            llm = llm_fn()
            if llm is None:
                continue

            # 프로바이더별 PDF 메시지 형식
            if prov_name == "claude":
                content = [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": prompt},
                ]
            elif prov_name == "gemini":
                content = [
                    {
                        "type": "image_url",
                        "image_url": f"data:application/pdf;base64,{b64}",
                    },
                    {"type": "text", "text": prompt},
                ]
            else:
                continue

            message = HumanMessage(content=content)
            response = llm.invoke([message])
            return response.content if hasattr(response, "content") else str(response)
        except Exception as e:
            errors.append(f"{prov_name}: {e}")
            continue

    # 모든 네이티브 시도 실패 → 텍스트 추출 fallback
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        doc.close()
        if text.strip():
            return call_llm(f"{prompt}\n\n텍스트:\n{text[:12000]}")
    except Exception as e:
        errors.append(f"text_fallback: {e}")

    return f'{{"error": "PDF 분석 실패: {"; ".join(errors)}"}}'
