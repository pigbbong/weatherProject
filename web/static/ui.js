// // 전역 상태
// window.currentGroup  = "ultrashort";
// window.currentLayout = "cond";


function initStateFromURL() {
    const path = location.pathname.split("/").filter(Boolean);

    currentGroup  = "ultrashort";
    currentLayout = "cond";

    if (path[0] === "ultrashort") {
        currentGroup  = "ultrashort";
        currentLayout = path[1] || "cond";
    }

    if (path[0] === "short") {
        currentGroup  = path[1] || "today";
        currentLayout = path[2] || "cond";
    }
}

// 그룹 선택
function selectGroup(group, event) {
    currentGroup = group;

    document
        .querySelectorAll("#group-buttons button")
        .forEach(btn => btn.classList.remove("active"));

    if (event && event.target) {
        event.target.classList.add("active");
    }

    updateURL();
    loadWeatherData();
}

// 레이아웃 선택
function selectLayout(layout) {
    currentLayout = layout;
    updateURL();
    loadWeatherData();
}

// URL 갱신 (화면용)
function updateURL() {
    if (currentGroup === "ultrashort") {
        history.pushState({}, "", `/ultrashort/${currentLayout}`);
    } else {
        history.pushState({}, "", `/short/${currentGroup}/${currentLayout}`);
    }
}


window.addEventListener("DOMContentLoaded", () => {
    // URL → 상태 복원
    initStateFromURL();

    // 날짜 버튼 active
    document
        .querySelectorAll("#group-buttons button")
        .forEach(btn => {
            btn.classList.toggle(
                "active",
                btn.dataset.group === currentGroup
            );
        });

    // 레이아웃 드롭다운 복원
    const layoutSelect = document.querySelector("#layout-select");
    if (layoutSelect) {
        layoutSelect.value = currentLayout;
    }

    // 상태가 다 맞춰진 다음에만 데이터 로드
    loadWeatherData();
});