const groupMap = {
    "now": "btn-now",
    "tomorrow_am": "btn-tam",
    "tomorrow_pm": "btn-tpm",
    "dayafter_am": "btn-dam",
    "dayafter_pm": "btn-dpm"
};

function applyInitialUI() {
    const group = INITIAL_GROUP || "now";
    let layout = INITIAL_LAYOUT || "cond";

    // 버튼 active 처리
    document.querySelectorAll("#group-buttons button")
        .forEach(btn => btn.classList.remove("active"));

    const btn = document.getElementById(groupMap[group]);
    if (btn) btn.classList.add("active");

    const select = document.getElementById("layout-select");
    if (select) select.value = layout;

    // 시간대별 허용되지 않는 layout 자동 보정
    if (group === "now" && layout === "rain") {
        layout = "detail";
        select.value = "detail";
    }
    if (group !== "now" && layout === "detail") {
        layout = "rain";
        select.value = "rain";
    }

    // 시간대별 드롭다운 옵션 숨기기
    for (let opt of select.options) {
        opt.hidden = false;
    }

    if (group === "now") {
        const rainOpt = select.querySelector('option[value="rain"]');
        if (rainOpt) rainOpt.hidden = true;
    } else {
        const detailOpt = select.querySelector('option[value="detail"]');
        if (detailOpt) detailOpt.hidden = true;
    }

    select.value = layout;
}

window.addEventListener("DOMContentLoaded", applyInitialUI);



// 그룹 버튼 클릭 시 URL 이동
window.selectGroup = function (group) {
    document.querySelectorAll("#group-buttons button")
        .forEach(btn => btn.classList.remove("active"));

    const btn = document.getElementById(groupMap[group]);
    if (btn) btn.classList.add("active");

    let layout = document.getElementById("layout-select").value;

    // 그룹 변경 시 layout 자동 보정
    if (group === "now" && layout === "rain") {
        layout = "detail";
    }
    if (group !== "now" && layout === "detail") {
        layout = "rain";
    }

    window.location.href = `/${group}/${layout}`;
};



// 레이아웃 드롭다운 변경 시 URL 이동
window.selectLayout = function (layout) {

    let group = "now";

    const active = document.querySelector("#group-buttons button.active");
    if (active) {
        switch (active.id) {
            case "btn-tam": group = "tomorrow_am"; break;
            case "btn-tpm": group = "tomorrow_pm"; break;
            case "btn-dam": group = "dayafter_am"; break;
            case "btn-dpm": group = "dayafter_pm"; break;
            case "btn-now":
            default: group = "now";
        }
    }

    // 허용되지 않는 layout 선택 시 자동 보정
    if (group === "now" && layout === "rain") {
        layout = "detail";
    }
    if (group !== "now" && layout === "detail") {
        layout = "rain";
    }

    window.location.href = `/${group}/${layout}`;
};

// 실시간 날씨 데이터 초기화
function resetLiveWeatherBox() {
    const loc = document.getElementById("loc");
    const icon = document.getElementById("icon");
    const status = document.getElementById("status");
    const temp = document.getElementById("temp");
    const yesterday = document.getElementById("yesterday");
    const feel = document.getElementById("feel");
    const humidity = document.getElementById("humidity");
    const rain = document.getElementById("rain");
    const wind = document.getElementById("wind");

    if (!loc) return;

    loc.textContent = "불러오는 중...";
    icon.src = "/icons/loading.png";
    status.textContent = "불러오는 중...";
    temp.textContent = "--℃";
    yesterday.textContent = "--";
    feel.textContent = "체감: --";
    humidity.textContent = "습도: --";
    rain.textContent = "강수: --";
    wind.textContent = "--";
}

// 실시간 날씨 박스를 업데이트
function updateLiveWeatherBox() {

    document.getElementById("live-loading").style.display = "block";
    document.getElementById("live-content").style.display = "none";

    fetch("/api/live_weather")
        .then(res => res.json())
        .then(data => {
            if (!data || data.error) return;

            document.getElementById("loc").textContent = data.location || "-";
            document.getElementById("icon").src = "/icons/" + data.icon;

            // 상태
            document.getElementById("status").textContent = data.status || "-";

            // 기온
            document.getElementById("temp").textContent =
                data.temperature ? `${data.temperature}℃` : "--℃";
            
            // 체감온도
            document.getElementById("feel").textContent = "체감: " + (data.feel || "--");

            // 습도 
            document.getElementById("humidity").textContent = "습도: " + (data.humidity || "--");

            // 강수
            document.getElementById("rain").textContent = "강수: " + (data.rainfall || "0.0mm");

            // 바람
            document.getElementById("wind").textContent =
                (data.wind_direction || "") + " " + (data.wind_speed || "");

            document.getElementById("live-loading").style.display = "none";
            document.getElementById("live-content").style.display = "block";
        })
        .catch(err => {
            console.log("Live weather load failed:", err);
        });
}

// 페이지 로드 + URL 변경 시 호출
window.updateLiveWeatherBox = updateLiveWeatherBox;