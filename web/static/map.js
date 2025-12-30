// map 전용 상태
// UI 상태는 ui.js에서 관리)
let weatherLayer = null;
let weatherDataJson = null;


function showMapMessage(message) {
    const container = document.getElementById("map-container");

    let el = document.getElementById("map-message");
    if (!el) {
        el = document.createElement("div");
        el.id = "map-message";
        container.appendChild(el);
    }

    el.innerText = message;
}

function hideMapMessage() {
    const el = document.getElementById("map-message");
    if (el) el.remove();
}


// 레이아웃 HTML
function labelHTML_cond(p) {
    return `
        <div class="weather-box">
            <div class="wb-city">${p["도시"]}</div>
            <div class="wb-icon">
                <img src="/static/icons/${p["아이콘"]}" width="20">
            </div>
            <div class="wb-cond">${p["날씨상태"]}</div>
            <div class="wb-temp">${p["기온"]}℃</div>
        </div>
    `;
}

function labelHTML_rain(p) {
    const isShort = p["강수확률"] !== undefined;
    const hasSnow =
        p["강설량"] &&
        p["강설량"] !== "0cm";

    return `
        <div class="weather-box-rain">
            <div class="rain-city">${p["도시"]}</div>
            <div class="rain-info">
                ${isShort ? `강수확률: ${p["강수확률"]}%<br>` : ""}
                최대강수량: ${p["강수량"]}
                ${hasSnow ? `<br>최대강설량: ${p["강설량"]}` : ""}
            </div>
        </div>
    `;
}

function labelHTML_wind(p) {
    return `
        <div class="weather-box-wind">
            <div class="wind-city">${p["도시"]}</div>
            <div class="wind-info">
                풍향: ${p["풍향"]}<br>
                풍속: ${p["풍속"]}m/s
            </div>
        </div>
    `;
}

function labelHTML_humid(p) {
    return `
        <div class="weather-box-humid">
            <div class="humid-city">${p["도시"]}</div>
            <div class="humid-info">
                습도: ${p["습도"]}%
            </div>
        </div>
    `;
}


// 도시 그룹 정의
const CITY_GROUP_1 = [
    "서울","부산","대전","대구","광주","백령도","울릉도","제주시","강릉"
];

const CITY_GROUP_2 = [
    "인천", "세종", "목포", "여수", "포항", "춘천", "서산", "전주", "울산"
];

const CITY_GROUP_3 = [
    "수원","고양","성남","속초","원주",
    "청주","천안","군산","순천","창원",
    "진주","경주","서귀포시"
];

let CITY_GROUP_4 = [];
let layoutRules = {};


// 도시 그룹 계산 + 레이아웃 규칙 생성
function computeGroups(features) {
    const used = new Set([...CITY_GROUP_1, ...CITY_GROUP_2, ...CITY_GROUP_3]);
    CITY_GROUP_4 = [];

features.forEach(f => {
    const name = f.properties["도시"];
    if (!used.has(name)) CITY_GROUP_4.push(name);
});

    layoutRules = {
        cond: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_cond },
            { cities: CITY_GROUP_2, minZoom: 8,  maxZoom: 20, html: labelHTML_cond },
            { cities: CITY_GROUP_3, minZoom: 9,  maxZoom: 20, html: labelHTML_cond },
            { cities: CITY_GROUP_4, minZoom: 11, maxZoom: 20, html: labelHTML_cond }
        ],
        rain: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_rain },
            { cities: CITY_GROUP_2, minZoom: 8,  maxZoom: 20, html: labelHTML_rain },
            { cities: CITY_GROUP_3, minZoom: 9,  maxZoom: 20, html: labelHTML_rain },
            { cities: CITY_GROUP_4, minZoom: 11, maxZoom: 20, html: labelHTML_rain }
        ],
        wind: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_wind },
            { cities: CITY_GROUP_2, minZoom: 8,  maxZoom: 20, html: labelHTML_wind },
            { cities: CITY_GROUP_3, minZoom: 9,  maxZoom: 20, html: labelHTML_wind },
            { cities: CITY_GROUP_4, minZoom: 11, maxZoom: 20, html: labelHTML_wind }
        ],
        humid: [
            { cities: CITY_GROUP_1, minZoom: 7,  maxZoom: 20, html: labelHTML_humid },
            { cities: CITY_GROUP_2, minZoom: 8,  maxZoom: 20, html: labelHTML_humid },
            { cities: CITY_GROUP_3, minZoom: 9,  maxZoom: 20, html: labelHTML_humid },
            { cities: CITY_GROUP_4, minZoom: 11, maxZoom: 20, html: labelHTML_humid }
        ]
    };
}


// 어떤 규칙을 적용할지 계산
function resolveRule(feature, zoom) {
    const rules = layoutRules[currentLayout];
    if (!rules) return null;

    const name = feature.properties["도시"];

    for (let rule of rules) {
        if (
            rule.cities.includes(name) &&
            zoom >= rule.minZoom &&
            zoom <= rule.maxZoom
        ) {
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


// 지도 초기화
const savedZoom = sessionStorage.getItem("mapZoom");
const savedCenter = sessionStorage.getItem("mapCenter");

const map = L.map("map", {
    center: savedCenter ? JSON.parse(savedCenter) : [35.8, 127.9],
    zoom: savedZoom ? Number(savedZoom) : 7
});

L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);


// 데이터 로딩
function loadWeatherData() {
    let apiURL;

    if (currentGroup === "ultrashort") {
        apiURL = `/api/ultrashort/${currentLayout}`;
    } else {
        apiURL = `/api/short/${currentGroup}/${currentLayout}`;
    }

    fetch(apiURL)
        .then(res => res.json())
        .then(data => {

            // 데이터 없음 → 메시지만 표시
            if (!data.features || data.features.length === 0) {

                // 이전 데이터 레이어만 제거
                if (weatherLayer) {
                    weatherLayer.remove();
                    weatherLayer = null;
                }

                weatherDataJson = null;

                showMapMessage("현재 데이터가 갱신중입니다. 잠시만 기다려주세요.");
                return;
            }

            // 데이터 있음 → 메시지 제거 + 기존 로직
            hideMapMessage();

            weatherDataJson = data;

            if (weatherLayer) weatherLayer.remove();

            computeGroups(data.features);

            weatherLayer = L.geoJSON(data, {
                pointToLayer: (feature, latlng) => {
                    const icon = createWeatherLabel(feature, map.getZoom());
                    return icon ? L.marker(latlng, { icon }) : null;
                }
            }).addTo(map);
        })
        .catch(err => {
            console.error("loadWeatherData error:", err);
            showMapMessage("데이터를 불러오는 중 오류가 발생했습니다.");
        });
}


// 줌 변경 시 재렌더링
map.on("zoomend moveend", () => {
    const zoom = map.getZoom();
    const center = map.getCenter();

    sessionStorage.setItem("mapZoom", zoom);
    sessionStorage.setItem("mapCenter", JSON.stringify([center.lat, center.lng]));

    if (!weatherDataJson) return;

    if (weatherLayer) weatherLayer.remove();

    weatherLayer = L.geoJSON(weatherDataJson, {
        pointToLayer: (feature, latlng) => {
            const icon = createWeatherLabel(feature, zoom);
            return icon ? L.marker(latlng, { icon }) : null;
        }
    }).addTo(map);
});
