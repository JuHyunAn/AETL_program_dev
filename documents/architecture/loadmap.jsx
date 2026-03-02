import { useState } from "react";

const engines = [
  {
    id: "metadata",
    label: "Metadata Engine",
    icon: "🗄️",
    isNew: false,
    color: "#3B82F6",
    features: [
      "Schema Crawler (자동 수집)",
      "Data Profiler (통계 프로파일링)",
      "Schema Diff Detector (변경 감지)",
      "Business Glossary (AI 용어 매핑)",
    ],
  },
  {
    id: "transform",
    label: "SQL / Transform Engine",
    icon: "⚙️",
    isNew: false,
    color: "#8B5CF6",
    features: [
      "NL-to-SQL (자연어 → SQL)",
      "Template Engine (SCD, 증분 등)",
      "DB-Specific SQL Rewriter",
      "Mapping Manager (소스-타겟)",
    ],
  },
  {
    id: "execution",
    label: "Execution Engine",
    icon: "▶️",
    isNew: true,
    color: "#EF4444",
    features: [
      "★ Live SQL 실행 (결과 즉시 반환)",
      "★ AI 자동 진단 (Root Cause)",
      "★ 수정 SQL 자동 생성 + 원클릭 실행",
      "★ Safety Guard Rails (승인/롤백)",
    ],
  },
  {
    id: "validation",
    label: "Validation Engine",
    icon: "✅",
    isNew: false,
    color: "#10B981",
    features: [
      "3-Tier 검증 (기술/정합성/비즈니스)",
      "AI 규칙 자동 제안",
      "일괄 검증 실행 + 리포트",
      "★ Auto-Fix 연동 (Execution Engine)",
    ],
  },
  {
    id: "export",
    label: "Export Engine",
    icon: "📥",
    isNew: true,
    color: "#F59E0B",
    features: [
      "★ 매핑정의서 Excel 자동 생성",
      "★ DDL Script 원클릭 생성",
      "★ 검증 리포트 Excel 생성",
      "★ 설계서 → 표준 양식 자동 기입",
    ],
  },
  {
    id: "designer",
    label: "DW Designer Engine",
    icon: "🏗️",
    isNew: true,
    color: "#EC4899",
    features: [
      "★ API 문서 → 엔티티 자동 분석",
      "★ ODS/DW/DM Star Schema 설계",
      "★ ERD 시각화 (인터랙티브)",
      "★ 설계 → DDL/SQL 원클릭 생성",
    ],
  },
  {
    id: "lineage",
    label: "Lineage Engine",
    icon: "🔗",
    isNew: false,
    color: "#06B6D4",
    features: [
      "SQL 파싱 기반 컬럼 리니지",
      "Forward/Backward Impact",
      "인터랙티브 DAG 시각화",
      "스키마 변경 영향도 분석",
    ],
  },
  {
    id: "monitor",
    label: "Monitor Engine",
    icon: "📊",
    isNew: false,
    color: "#6366F1",
    features: [
      "Job 상태 추적 + SLA",
      "AI Troubleshooter",
      "Confidence 기반 자동 복구",
      "Slack/Email 알림 연동",
    ],
  },
];

const scenarios = [
  {
    id: "mapping",
    title: "매핑정의서 자동화",
    icon: "📋",
    color: "#F59E0B",
    steps: [
      { engine: "metadata", action: "소스/타겟 스키마 자동 수집", type: "auto" },
      { engine: "transform", action: "컬럼 매핑 + 적재 SQL 생성", type: "auto" },
      { engine: "validation", action: "검증 SQL 자동 생성", type: "auto" },
      { engine: "export", action: "엑셀 템플릿에 자동 기입 → 다운로드", type: "deliver" },
    ],
  },
  {
    id: "livefix",
    title: "실행 + 자동 수정",
    icon: "🔧",
    color: "#EF4444",
    steps: [
      { engine: "validation", action: "검증 SQL 준비 완료", type: "auto" },
      { engine: "execution", action: "[▶ 실행] → 건수 불일치 3건 발견", type: "execute" },
      { engine: "execution", action: "AI 진단: NULL PK 3건 → 원인 특정", type: "diagnose" },
      { engine: "execution", action: "[수정 적용] → 재적재 → 재검증 PASS ✅", type: "fix" },
    ],
  },
  {
    id: "dwdesign",
    title: "API → DW 설계",
    icon: "🏗️",
    color: "#EC4899",
    steps: [
      { engine: "designer", action: "API 문서(Swagger/PDF) 파싱", type: "auto" },
      { engine: "designer", action: "엔티티 분석 → Star Schema 설계", type: "auto" },
      { engine: "designer", action: "ERD 시각화 (ODS/DW/DM)", type: "visual" },
      { engine: "export", action: "DDL + 변환SQL + 매핑정의서 생성", type: "deliver" },
    ],
  },
];

const stepTypeStyles = {
  auto: { bg: "bg-blue-50", border: "border-blue-300", badge: "bg-blue-500", label: "Auto" },
  execute: { bg: "bg-red-50", border: "border-red-300", badge: "bg-red-500", label: "Execute" },
  diagnose: { bg: "bg-orange-50", border: "border-orange-300", badge: "bg-orange-500", label: "AI 분석" },
  fix: { bg: "bg-green-50", border: "border-green-300", badge: "bg-green-500", label: "Auto-Fix" },
  deliver: { bg: "bg-yellow-50", border: "border-yellow-300", badge: "bg-yellow-600", label: "산출물" },
  visual: { bg: "bg-purple-50", border: "border-purple-300", badge: "bg-purple-500", label: "시각화" },
};

const automationData = [
  { task: "소스 테이블 분석", before: 95, after: 10, category: "메타데이터" },
  { task: "매핑정의서 작성", before: 90, after: 15, category: "문서화" },
  { task: "적재 SQL 작성", before: 80, after: 25, category: "변환" },
  { task: "검증 SQL 작성+실행", before: 85, after: 15, category: "검증" },
  { task: "오류 분석+수정", before: 70, after: 30, category: "트러블슈팅" },
  { task: "DW 모델 설계", before: 60, after: 30, category: "설계" },
  { task: "DDL 작성", before: 50, after: 5, category: "문서화" },
];

export default function AETLv2Dashboard() {
  const [activeTab, setActiveTab] = useState("overview");
  const [selectedEngine, setSelectedEngine] = useState(null);
  const [activeScenario, setActiveScenario] = useState(null);
  const [activeStep, setActiveStep] = useState(-1);

  const tabs = [
    { id: "overview", label: "Architecture", icon: "◈" },
    { id: "scenarios", label: "Scenarios", icon: "▸" },
    { id: "automation", label: "Automation", icon: "◉" },
    { id: "comparison", label: "v1 vs v2", icon: "⟺" },
  ];

  return (
    <div style={{ fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace" }} className="min-h-screen bg-gray-950 text-gray-100">
      {/* Header */}
      <div className="border-b border-gray-800 px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded bg-gradient-to-br from-red-500 to-orange-500 flex items-center justify-center text-sm font-bold">A</div>
            <div>
              <h1 className="text-lg font-bold tracking-tight">AETL v2 Architecture</h1>
              <p className="text-xs text-gray-500 tracking-wide">GENERATE → EXECUTE → DELIVER</p>
            </div>
          </div>
          <div className="flex gap-1">
            {tabs.map((t) => (
              <button
                key={t.id}
                onClick={() => { setActiveTab(t.id); setSelectedEngine(null); setActiveScenario(null); setActiveStep(-1); }}
                className={`px-3 py-1.5 text-xs rounded transition-all ${
                  activeTab === t.id ? "bg-gray-100 text-gray-950 font-bold" : "text-gray-400 hover:text-gray-200 hover:bg-gray-800"
                }`}
              >
                <span className="mr-1.5">{t.icon}</span>{t.label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="p-6">
        {/* Tab: Overview */}
        {activeTab === "overview" && (
          <div>
            <div className="mb-6 flex items-center gap-4">
              <p className="text-sm text-gray-400">엔진을 클릭하면 상세 기능을 확인할 수 있습니다</p>
              <div className="flex items-center gap-2 text-xs">
                <span className="px-2 py-0.5 rounded bg-red-500/20 text-red-400 border border-red-500/30">★ NEW in v2</span>
                <span className="px-2 py-0.5 rounded bg-gray-700 text-gray-300">기존 엔진</span>
              </div>
            </div>

            {/* Architecture Grid */}
            <div className="grid grid-cols-4 gap-3 mb-6">
              {engines.map((e) => (
                <button
                  key={e.id}
                  onClick={() => setSelectedEngine(selectedEngine === e.id ? null : e.id)}
                  className={`relative p-4 rounded-lg border text-left transition-all ${
                    selectedEngine === e.id
                      ? "border-gray-400 bg-gray-800 shadow-lg shadow-gray-900"
                      : e.isNew
                      ? "border-red-500/30 bg-red-500/5 hover:bg-red-500/10 hover:border-red-500/50"
                      : "border-gray-700 bg-gray-900 hover:bg-gray-800 hover:border-gray-600"
                  }`}
                >
                  {e.isNew && (
                    <span className="absolute -top-2 -right-2 text-[10px] px-1.5 py-0.5 bg-red-500 text-white rounded font-bold">NEW</span>
                  )}
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-lg">{e.icon}</span>
                    <span className="text-xs font-bold" style={{ color: e.color }}>{e.label}</span>
                  </div>
                  <p className="text-[10px] text-gray-500 leading-relaxed">
                    {e.features[0].replace("★ ", "")}
                  </p>
                </button>
              ))}
            </div>

            {/* Selected Engine Detail */}
            {selectedEngine && (
              <div className="border border-gray-700 rounded-lg p-5 bg-gray-900/50">
                {(() => {
                  const e = engines.find((x) => x.id === selectedEngine);
                  return (
                    <div>
                      <div className="flex items-center gap-3 mb-4">
                        <span className="text-2xl">{e.icon}</span>
                        <div>
                          <h3 className="font-bold" style={{ color: e.color }}>{e.label}</h3>
                          {e.isNew && <span className="text-[10px] text-red-400">v2에서 새로 추가됨</span>}
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        {e.features.map((f, i) => (
                          <div key={i} className={`text-xs px-3 py-2 rounded ${
                            f.startsWith("★") ? "bg-red-500/10 text-red-300 border border-red-500/20" : "bg-gray-800 text-gray-300"
                          }`}>
                            {f}
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })()}
              </div>
            )}

            {/* Flow Diagram */}
            {!selectedEngine && (
              <div className="border border-gray-800 rounded-lg p-5 bg-gray-900/30">
                <p className="text-xs text-gray-500 mb-4 font-bold tracking-wider">INTEGRATED FLOW</p>
                <div className="flex items-center justify-center gap-2 flex-wrap">
                  {[
                    { label: "User Input", color: "gray", sub: "자연어/파일" },
                    null,
                    { label: "Metadata", color: "#3B82F6", sub: "분석" },
                    null,
                    { label: "Transform", color: "#8B5CF6", sub: "생성" },
                    null,
                    { label: "Execute", color: "#EF4444", sub: "실행+수정", isNew: true },
                    null,
                    { label: "Validate", color: "#10B981", sub: "검증" },
                    null,
                    { label: "Export", color: "#F59E0B", sub: "산출물", isNew: true },
                  ].map((item, i) =>
                    item === null ? (
                      <span key={i} className="text-gray-600 text-lg">→</span>
                    ) : (
                      <div key={i} className={`px-3 py-2 rounded text-center border ${
                        item.isNew ? "border-red-500/40 bg-red-500/10" : "border-gray-700 bg-gray-800"
                      }`}>
                        <div className="text-xs font-bold" style={{ color: typeof item.color === "string" && item.color !== "gray" ? item.color : "#9CA3AF" }}>
                          {item.label}
                        </div>
                        <div className="text-[10px] text-gray-500">{item.sub}</div>
                      </div>
                    )
                  )}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Tab: Scenarios */}
        {activeTab === "scenarios" && (
          <div>
            <p className="text-sm text-gray-400 mb-5">핵심 시나리오를 선택하면 단계별 실행 흐름을 확인할 수 있습니다</p>

            <div className="grid grid-cols-3 gap-3 mb-6">
              {scenarios.map((s) => (
                <button
                  key={s.id}
                  onClick={() => { setActiveScenario(activeScenario === s.id ? null : s.id); setActiveStep(-1); }}
                  className={`p-4 rounded-lg border text-left transition-all ${
                    activeScenario === s.id
                      ? "border-gray-400 bg-gray-800"
                      : "border-gray-700 bg-gray-900 hover:bg-gray-800"
                  }`}
                >
                  <div className="flex items-center gap-2 mb-2">
                    <span className="text-xl">{s.icon}</span>
                    <span className="text-sm font-bold" style={{ color: s.color }}>{s.title}</span>
                  </div>
                  <p className="text-[10px] text-gray-500">{s.steps.length}단계 자동화 프로세스</p>
                </button>
              ))}
            </div>

            {activeScenario && (() => {
              const s = scenarios.find((x) => x.id === activeScenario);
              return (
                <div className="border border-gray-700 rounded-lg p-5 bg-gray-900/50">
                  <div className="flex items-center gap-3 mb-5">
                    <span className="text-2xl">{s.icon}</span>
                    <div>
                      <h3 className="font-bold" style={{ color: s.color }}>{s.title}</h3>
                      <p className="text-[10px] text-gray-500">단계를 클릭하여 상세 확인</p>
                    </div>
                  </div>

                  <div className="space-y-3">
                    {s.steps.map((step, i) => {
                      const st = stepTypeStyles[step.type];
                      const engine = engines.find((e) => e.id === step.engine);
                      return (
                        <button
                          key={i}
                          onClick={() => setActiveStep(activeStep === i ? -1 : i)}
                          className={`w-full flex items-center gap-4 p-3 rounded-lg border transition-all text-left ${
                            activeStep === i ? `${st.bg} ${st.border} border` : "bg-gray-800 border-gray-700 hover:border-gray-600"
                          }`}
                        >
                          <div className="flex items-center justify-center w-7 h-7 rounded-full bg-gray-700 text-xs font-bold text-gray-300 shrink-0">
                            {i + 1}
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-0.5">
                              <span className="text-sm">{engine.icon}</span>
                              <span className="text-[10px] font-bold" style={{ color: engine.color }}>{engine.label}</span>
                            </div>
                            <p className={`text-xs ${activeStep === i ? "text-gray-800" : "text-gray-300"}`}>{step.action}</p>
                          </div>
                          <span className={`text-[10px] px-2 py-0.5 rounded text-white shrink-0 ${st.badge}`}>
                            {st.label}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })()}
          </div>
        )}

        {/* Tab: Automation Impact */}
        {activeTab === "automation" && (
          <div>
            <p className="text-sm text-gray-400 mb-5">AETL v2 도입 시 작업별 수작업 시간 절감 효과</p>

            <div className="space-y-4">
              {automationData.map((d, i) => {
                const savings = d.before - d.after;
                return (
                  <div key={i} className="border border-gray-800 rounded-lg p-4 bg-gray-900/30">
                    <div className="flex items-center justify-between mb-3">
                      <div>
                        <span className="text-sm font-bold text-gray-200">{d.task}</span>
                        <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded bg-gray-800 text-gray-400">{d.category}</span>
                      </div>
                      <span className="text-sm font-bold text-green-400">-{savings}%</span>
                    </div>

                    <div className="space-y-2">
                      <div className="flex items-center gap-3">
                        <span className="text-[10px] text-gray-500 w-12 shrink-0">Before</span>
                        <div className="flex-1 h-5 bg-gray-800 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-red-500/60 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${d.before}%` }}
                          >
                            <span className="text-[10px] text-white font-bold">{d.before}%</span>
                          </div>
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-[10px] text-gray-500 w-12 shrink-0">After</span>
                        <div className="flex-1 h-5 bg-gray-800 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-green-500/60 rounded-full flex items-center justify-end pr-2"
                            style={{ width: `${Math.max(d.after, 8)}%` }}
                          >
                            <span className="text-[10px] text-white font-bold">{d.after}%</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            <div className="mt-6 border border-green-500/30 rounded-lg p-4 bg-green-500/5">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-green-400 font-bold text-sm">평균 수작업 절감율</span>
              </div>
              <div className="text-3xl font-bold text-green-400">
                {Math.round(automationData.reduce((sum, d) => sum + (d.before - d.after), 0) / automationData.length)}%
              </div>
              <p className="text-[10px] text-gray-500 mt-1">수작업 비율 기준 (Before → After 평균 감소)</p>
            </div>
          </div>
        )}

        {/* Tab: v1 vs v2 */}
        {activeTab === "comparison" && (
          <div>
            <div className="grid grid-cols-2 gap-4">
              {/* v1 */}
              <div className="border border-gray-700 rounded-lg p-5 bg-gray-900/50">
                <div className="flex items-center gap-2 mb-4">
                  <span className="px-2 py-0.5 rounded bg-gray-700 text-gray-300 text-xs font-bold">v1</span>
                  <span className="text-sm text-gray-400">보조 도구 수준</span>
                </div>
                <div className="space-y-3">
                  {[
                    { step: "메타데이터 분석", status: "○" },
                    { step: "SQL 생성", status: "○" },
                    { step: "화면에 보여줌", status: "○" },
                    { step: "직접 실행", status: "✕", dim: true },
                    { step: "오류 자동 수정", status: "✕", dim: true },
                    { step: "매핑정의서 자동화", status: "✕", dim: true },
                    { step: "DW 설계 추천", status: "✕", dim: true },
                    { step: "산출물 생성", status: "✕", dim: true },
                  ].map((item, i) => (
                    <div key={i} className={`flex items-center gap-3 text-xs ${item.dim ? "text-gray-600" : "text-gray-300"}`}>
                      <span className={`w-5 h-5 flex items-center justify-center rounded text-[10px] font-bold ${
                        item.dim ? "bg-gray-800 text-gray-600" : "bg-blue-500/20 text-blue-400"
                      }`}>
                        {item.status}
                      </span>
                      {item.step}
                    </div>
                  ))}
                </div>
                <div className="mt-4 pt-3 border-t border-gray-800">
                  <p className="text-[10px] text-gray-600">끝 = 사용자가 SQL 복사 → 수동 실행</p>
                </div>
              </div>

              {/* v2 */}
              <div className="border border-red-500/30 rounded-lg p-5 bg-red-500/5">
                <div className="flex items-center gap-2 mb-4">
                  <span className="px-2 py-0.5 rounded bg-red-500 text-white text-xs font-bold">v2</span>
                  <span className="text-sm text-gray-300">실행 플랫폼 수준</span>
                </div>
                <div className="space-y-3">
                  {[
                    { step: "메타데이터 분석", status: "●" },
                    { step: "SQL 생성", status: "●" },
                    { step: "직접 실행 + 결과 반환", status: "●", isNew: true },
                    { step: "AI 오류 진단 + 자동 수정", status: "●", isNew: true },
                    { step: "매핑정의서 Excel 자동 생성", status: "●", isNew: true },
                    { step: "API → DW 설계 추천", status: "●", isNew: true },
                    { step: "DDL 원클릭 생성 + 검증 리포트", status: "●", isNew: true },
                    { step: "검증 리포트 산출물", status: "●", isNew: true },
                  ].map((item, i) => (
                    <div key={i} className={`flex items-center gap-3 text-xs ${item.isNew ? "text-red-300" : "text-gray-300"}`}>
                      <span className={`w-5 h-5 flex items-center justify-center rounded text-[10px] font-bold ${
                        item.isNew ? "bg-red-500/30 text-red-300" : "bg-blue-500/20 text-blue-400"
                      }`}>
                        {item.status}
                      </span>
                      {item.step}
                      {item.isNew && <span className="text-[9px] px-1 py-0.5 bg-red-500/20 rounded text-red-400">NEW</span>}
                    </div>
                  ))}
                </div>
                <div className="mt-4 pt-3 border-t border-red-500/20">
                  <p className="text-[10px] text-red-400/70">끝 = 산출물 다운로드 + 검증 완료</p>
                </div>
              </div>
            </div>

            {/* Philosophy */}
            <div className="mt-6 border border-gray-800 rounded-lg p-5 bg-gray-900/30">
              <p className="text-xs text-gray-500 mb-3 font-bold tracking-wider">CORE PHILOSOPHY SHIFT</p>
              <div className="flex items-center justify-center gap-4">
                <div className="text-center px-5 py-3 rounded bg-gray-800 border border-gray-700">
                  <p className="text-lg font-bold text-gray-400">Generate</p>
                  <p className="text-[10px] text-gray-600">SQL을 보여준다</p>
                </div>
                <span className="text-gray-600 text-2xl">→</span>
                <div className="text-center px-5 py-3 rounded bg-red-500/10 border border-red-500/30">
                  <p className="text-lg font-bold text-red-400">Execute</p>
                  <p className="text-[10px] text-red-400/60">실행하고 수정한다</p>
                </div>
                <span className="text-gray-600 text-2xl">→</span>
                <div className="text-center px-5 py-3 rounded bg-yellow-500/10 border border-yellow-500/30">
                  <p className="text-lg font-bold text-yellow-400">Deliver</p>
                  <p className="text-[10px] text-yellow-400/60">산출물을 만든다</p>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
