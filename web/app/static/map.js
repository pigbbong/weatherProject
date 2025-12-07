// HTML 생성 함수
// 날씨상태 레이아웃
function labelHTML_cond(p) {

    let tempText = `${p["기온"]}℃`;

    switch (currentGroup) {
        case "tomorrow_am":
        case "dayafter_am":
            tempText += " (최저)";
            break;

        case "tomorrow_pm":
        case "dayafter_pm":
            tempText += " (최고)";
            break;

        case "now":
        default:
            break;
    }

    return `
        <div class="weather-box">
            <div class="wb-city">${p["도시"]}</div>
            <div class="wb-icon">
                <img src="/icons/${p["아이콘"]}" width="20">
            </div>
            <div class="wb-cond">${p["날씨상태"]}</div>
            <div class="wb-temp">${tempText}</div>
        </div>
    `;
}

// 미세먼지 레이아웃
function labelHTML_dust(p) {

    // 미세먼지 색상
    function colorDust(v) {
        if (!v) return "#000000"; // 기본값

        const txt = v.toString();

        if (txt.includes("좋음")) {
            return "#0000FF";
        }
        if (txt.includes("보통")) {
            return "#228B22";
        }
        if (txt.includes("나쁨") && !txt.includes("매우")) {
            return "#FFA500";
        }
        if (txt.includes("매우나쁨")) {
            return "#FF0000";
        }

        return "#000000";
    }

    return `
        <div class="weather-box-dust">
            <div class="wb-city">${p["도시"]}</div>

            <div class="wb-cond" style="color:${colorDust(p["미세먼지"])};">
                미세먼지: ${p["미세먼지"]}
            </div>

            <div class="wb-cond" style="color:${colorDust(p["초미세먼지"])};">
                초미세먼지: ${p["초미세먼지"]}
            </div>
        </div>
    `;
}


// 상세정보 레이아웃
function labelHTML_detail(p) {
    return `
        <div class="weather-box-detail">

            <div class="detail-city">${p["도시"]}</div>

            <div class="detail-info">
                습도: ${p["습도"]}<br>
                강수: ${p["강수"]}mm<br>
                풍향: ${p["풍향"]}<br>
                풍속: ${p["풍속"]}<br>
                자외선: ${p["자외선"]}
            </div>

        </div>
    `;
}


// 강수 레이아웃
function labelHTML_rain(p) {
    return `
        <div class="weather-box-rain">

            <div class="rain-city">${p["도시"]}</div>

            <div class="rain-info">
                강수확률: ${p["강수확률"]}% <br>
                최대강수량: ${p["최대강수량"]}mm
            </div>

        </div>
    `;
}


// 외출지수 레이아웃
function labelHTML_score(p) {

    function colorScore(v) {
        if (!v) return "#000000";

        const txt = v.toString();

        if (txt.includes("외출하기 좋음")) return "#4A90E2";
        if (txt.includes("외출하기 괜찮음")) return "#7ED321";
        if (txt.includes("외출양호")) return "#E5C100";
        if (txt.includes("외출하기 별로임")) return "#F5A623";
        if (txt.includes("외출하기 안좋음")) return "#D0021B";
        return "#000000";
    }

    return `
        <div class="weather-box-score">
            <div class="wb-city">${p["도시"]}</div>
            <div class="wb-cond" style="color:${colorScore(p["등급"])};">
                ${p["등급"]}
            </div>
        </div>
    `;
}


// 도시 그룹 정의
const CITY_GROUP_1 = [
    "서울","인천","부산","대전","대구",
    "광주","백령도","울릉도","제주시",
    "전주","목포","여수","포항","울산",
    "춘천","강릉","서산"
];

const CITY_GROUP_2 = [
    "수원","고양","성남","속초","원주",
    "청주","천안","군산","순천","창원",
    "진주","경주","서귀포시"
];

let CITY_GROUP_3 = [];  // 나머지 도시들 자동 계산

let currentLayout = "cond";
let weatherLayer = null;

// CITY_GROUP_3 자동 생성 + layoutRules 구성
function computeGroups(features) {
    const used = new Set([...CITY_GROUP_1, ...CITY_GROUP_2]);
    CITY_GROUP_3 = [];

    features.forEach(f => {
        const name = f.properties["도시"];
        if (!used.has(name)) CITY_GROUP_3.push(name);
    });

    layoutRules = {
        cond: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_cond },
            { cities: CITY_GROUP_2, minZoom: 9,  maxZoom: 20, html: labelHTML_cond },
            { cities: CITY_GROUP_3, minZoom: 10, maxZoom: 20, html: labelHTML_cond }
        ],

        dust: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_dust },
            { cities: CITY_GROUP_2, minZoom: 9,  maxZoom: 20, html: labelHTML_dust },
            { cities: CITY_GROUP_3, minZoom: 10, maxZoom: 20, html: labelHTML_dust }
        ],
        detail: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_detail },
            { cities: CITY_GROUP_2, minZoom: 9,  maxZoom: 20, html: labelHTML_detail },
            { cities: CITY_GROUP_3, minZoom: 10, maxZoom: 20, html: labelHTML_detail }
        ],
        rain: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_rain },
            { cities: CITY_GROUP_2, minZoom: 9,  maxZoom: 20, html: labelHTML_rain },
            { cities: CITY_GROUP_3, minZoom: 10, maxZoom: 20, html: labelHTML_rain }
        ],
        score: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_score },
            { cities: CITY_GROUP_2, minZoom: 9,  maxZoom: 20, html: labelHTML_score },
            { cities: CITY_GROUP_3, minZoom: 10, maxZoom: 20, html: labelHTML_score }
        ]
    };
}

let layoutRules = { cond: [] };

// 어떤 규칙을 적용할지 계산
function resolveRule(feature, zoom) {
    const name = feature.properties["도시"];
    const rules = layoutRules[currentLayout];

    for (let rule of rules) {
        if (rule.cities.includes(name) &&
            zoom >= rule.minZoom &&
            zoom <= rule.maxZoom) {
            return rule;
        }
    }
    return null;
}

// Marker 아이콘 생성
function createWeatherLabel(feature, zoom) {
    const rule = resolveRule(feature, zoom);
    if (!rule) return null;

    return L.divIcon({
        html: rule.html(feature.properties),
        className: "",
        iconSize: [55, 70],
        iconAnchor: [27, 35]
    });
}

// 지도 생성 
const map = L.map("map", {
    center: [35.8, 127.9],
    zoom: 7
});

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

// DB에서 데이터 가져오기
function loadWeatherData() {
    const group  = INITIAL_GROUP;
    const layout = INITIAL_LAYOUT;

    currentGroup = group;   // 그룹 변경 적용
    currentLayout = layout; // 레이아웃 변경 적용

    fetch(`/api/weather/${group}/${layout}`)
        .then(res => res.json())
        .then(json => {
            weatherDataJson = json;

            computeGroups(json.features);

            if (weatherLayer) {
                weatherLayer.remove();
            }

            weatherLayer = L.geoJSON(json, {
                pointToLayer: (feature, latlng) => {
                    const zoom = map.getZoom();
                    const icon = createWeatherLabel(feature, zoom);
                    return icon ? L.marker(latlng, { icon }) : null;
                }
            }).addTo(map);
        });
}

// zoom 변경 시 아이콘 갱신
map.on("zoomend", () => {
    if (!weatherDataJson) return;

    const zoom = map.getZoom();

    // 기존 레이어 제거
    if (weatherLayer) {
        weatherLayer.remove();
    }

    // 새로운 레이어 생성
    weatherLayer = L.geoJSON(weatherDataJson, {
        pointToLayer: (feature, latlng) => {
            const icon = createWeatherLabel(feature, zoom);
            return icon ? L.marker(latlng, { icon }) : null;
        }
    }).addTo(map);
});

// 실시간 날씨 불러오기
function loadLiveWeather() {
    fetch("/api/live_weather")
        .then(res => res.json())
        .then(data => {
            if (!data || data.error) return;

            const box = document.getElementById("live-weather-box");
            if (!box) return;

            // 어제보다 변화 텍스트 조합
            const yesterdayText =
                (data.yesterday_value !== null && data.yesterday_status)
                    ? `어제보다 ${data.yesterday_value}° ${data.yesterday_status}`
                    : "";

            // HTML 재배치
            box.innerHTML = `
                <div class="live-top-line">
                    <div class="live-location">${data.location || "-"}</div>
                </div>

                <div class="live-main">
                    <img src="/icons/${data.icon}" class="live-icon">

                    <div class="live-temp-block">
                        <div class="live-temp-line">
                            ${data.temperature || "-"}˚ 
                        </div>

                        <div class="live-status-line">
                            ${data.status || ""}
                        </div>
                    </div>
                </div>

                <div class="live-sub">
                    <div>체감온도: ${data.feel || "-"}</div>
                    <div>습도: ${data.humidity || "-"}</div>
                    <div>강수: ${data.rainfall || "0.0mm"}</div>
                    <div>${data.wind_direction || ""} ${data.wind_speed || ""}</div>
                </div>
            `;
        })
        .catch(err => console.log("live weather error", err));
}

// 초기 실행 
loadWeatherData();
loadLiveWeather();