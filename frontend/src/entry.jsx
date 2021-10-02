const { h, render, Fragment } = require('preact');
const { useState, useRef } = require('preact/hooks');
const uPlot = require('uplot');

function fmtUSD(val, dec) {
  return "$" + val.toFixed(dec).replace(/\d(?=(\d{3})+(?:\.|$))/g, "$&,");
}

// column-highlights the hovered x index
function columnHighlightPlugin({ className, style = {backgroundColor: "rgba(51,204,255,0.3)"} } = {}) {
  let underEl, overEl, highlightEl, currIdx;

  function init(u) {
    underEl = u.under;
    overEl = u.over;

    highlightEl = document.createElement("div");

    className && highlightEl.classList.add(className);

    uPlot.assign(highlightEl.style, {
      pointerEvents: "none",
      display: "none",
      position: "absolute",
      left: 0,
      top: 0,
      height: "100%",
      ...style
    });

    underEl.appendChild(highlightEl);

    // show/hide highlight on enter/exit
    overEl.addEventListener("mouseenter", () => {highlightEl.style.display = null;});
    overEl.addEventListener("mouseleave", () => {highlightEl.style.display = "none";});
  }

  function update(u) {
    if (currIdx !== u.cursor.idx) {
      currIdx = u.cursor.idx;

      let [iMin, iMax] = u.series[0].idxs;

      const dx    = iMax - iMin;
      const width = (u.bbox.width / dx) / devicePixelRatio;
      const xVal  = u.scales.x.distr == 2 ? currIdx : u.data[0][currIdx];
      const left  = u.valToPos(xVal, "x") - width / 2;

      highlightEl.style.transform = "translateX(" + Math.round(left) + "px)";
      highlightEl.style.width = Math.round(width) + "px";
    }
  }

  return {
    opts: (u, opts) => {
      uPlot.assign(opts, {
        cursor: {
          x: false,
          y: false,
        }
      });
    },
    hooks: {
      init: init,
      setCursor: update,
    }
  };
}

// converts the legend into a simple tooltip
function legendAsTooltipPlugin({ className, style = { backgroundColor:"rgba(255, 249, 196, 0.92)", color: "black" } } = {}) {
  let legendEl;

  function init(u, opts) {
    legendEl = u.root.querySelector(".u-legend");

    legendEl.classList.remove("u-inline");
    className && legendEl.classList.add(className);

    uPlot.assign(legendEl.style, {
      textAlign: "left",
      pointerEvents: "none",
      display: "none",
      position: "absolute",
      left: 0,
      top: 0,
      zIndex: 100,
      boxShadow: "2px 2px 10px rgba(0,0,0,0.5)",
      ...style
    });

    // hide series color markers
    const idents = legendEl.querySelectorAll(".u-marker");

    for (let i = 0; i < idents.length; i++)
      idents[i].style.display = "none";

    const overEl = u.over;
    overEl.style.overflow = "visible";

    // move legend into plot bounds
    overEl.appendChild(legendEl);

    // show/hide tooltip on enter/exit
    overEl.addEventListener("mouseenter", () => {legendEl.style.display = null;});
    overEl.addEventListener("mouseleave", () => {legendEl.style.display = "none";});

    // let tooltip exit plot
  //	overEl.style.overflow = "visible";
  }

  function update(u) {
    const { left, top } = u.cursor;
    legendEl.style.transform = "translate(" + left + "px, " + top + "px)";
  }

  return {
    hooks: {
      init: init,
      setCursor: update,
    }
  };
}

// draws candlestick symbols (expects data in OHLC order)
function candlestickPlugin({ gap = 2, shadowColor = "#000000", bearishColor = "#e54245", bullishColor = "#4ab650", bodyMaxWidth = 20, shadowWidth = 2, bodyOutline = 1 } = {}) {

  function drawCandles(u) {
    u.ctx.save();

    const offset = (shadowWidth % 2) / 2;

    u.ctx.translate(offset, offset);

    let [iMin, iMax] = u.series[0].idxs;

    let vol0AsY = u.valToPos(0, "vol", true);

    for (let i = iMin; i <= iMax; i++) {
      let xVal         = u.scales.x.distr == 2 ? i : u.data[0][i];
      let open         = u.data[1][i];
      let high         = u.data[2][i];
      let low          = u.data[3][i];
      let close        = u.data[4][i];
      let vol          = u.data[5][i];

      let timeAsX      = u.valToPos(xVal,  "x", true);
      let lowAsY       = u.valToPos(low,   "y", true);
      let highAsY      = u.valToPos(high,  "y", true);
      let openAsY      = u.valToPos(open,  "y", true);
      let closeAsY     = u.valToPos(close, "y", true);
      let volAsY       = u.valToPos(vol, "vol", true);


      // shadow rect
      let shadowHeight = Math.max(highAsY, lowAsY) - Math.min(highAsY, lowAsY);
      let shadowX      = timeAsX - (shadowWidth / 2);
      let shadowY      = Math.min(highAsY, lowAsY);

      u.ctx.fillStyle = shadowColor;
      u.ctx.fillRect(
        Math.round(shadowX),
        Math.round(shadowY),
        Math.round(shadowWidth),
        Math.round(shadowHeight),
      );

      // body rect
      let columnWidth  = u.bbox.width / (iMax - iMin);
      let bodyWidth    = Math.min(bodyMaxWidth, columnWidth - gap);
      let bodyHeight   = Math.max(closeAsY, openAsY) - Math.min(closeAsY, openAsY);
      let bodyX        = timeAsX - (bodyWidth / 2);
      let bodyY        = Math.min(closeAsY, openAsY);
      let bodyColor    = open > close ? bearishColor : bullishColor;

      u.ctx.fillStyle = shadowColor;
      u.ctx.fillRect(
        Math.round(bodyX),
        Math.round(bodyY),
        Math.round(bodyWidth),
        Math.round(bodyHeight),
      );

      u.ctx.fillStyle = bodyColor;
      u.ctx.fillRect(
        Math.round(bodyX + bodyOutline),
        Math.round(bodyY + bodyOutline),
        Math.round(bodyWidth - bodyOutline * 2),
        Math.round(bodyHeight - bodyOutline * 2),
      );

      // volume rect
      u.ctx.fillRect(
        Math.round(bodyX),
        Math.round(volAsY),
        Math.round(bodyWidth),
        Math.round(vol0AsY - volAsY),
      );
    }

    u.ctx.translate(-offset, -offset);

    u.ctx.restore();
  }

  return {
    opts: (u, opts) => {
      uPlot.assign(opts, {
        cursor: {
          points: {
            show: false,
          }
        }
      });

      opts.series.forEach(series => {
        series.paths = () => null;
        series.points = {show: false};
      });
    },
    hooks: {
      draw: drawCandles,
    }
  };
}

const fmtDate = uPlot.fmtDate("{YYYY}-{MM}-{DD}");
const tzDate = ts => uPlot.tzDate(new Date(ts * 1e3), "Etc/UTC");

const opts = {
  width: 400 * 8,
  height: 400,
  title: 'Untitled',
  plugins: [
    //columnHighlightPlugin(),
    //legendAsTooltipPlugin(),
    candlestickPlugin(),
  ],
  tzDate,
  scales: {
    x: {
      time: true,
      auto: false,
      //distr: 2,
    },
    vol: {
      range: [0, 2000],
    },
  },
  series: [
    {
      label: "Date",
      value: (u, ts) => fmtDate(tzDate(ts)),
    },
    {
      label: "Open",
      value: (u, v) => fmtUSD(v, 2),
    },
    {
      label: "High",
      value: (u, v) => fmtUSD(v, 2),
    },
    {
      label: "Low",
      value: (u, v) => fmtUSD(v, 2),
    },
    {
      label: "Close",
      value: (u, v) => fmtUSD(v, 2),
    },
    {
      label: "Volume",
      scale: 'vol',
    },
  ],
  axes: [
    {},
    {
      values: (u, vals) => vals.map(v => fmtUSD(v, 0)),
    },
    {
      side: 1,
      scale: 'vol',
      grid: {show: false},
    }
  ],
  legend: {
    show: false,
    live: false
  },
  cursor: {
    show: false
  }
};

const DataForm = ({ containerRef }) => {
  const [table, setTable] = useState('agg1d');
  const [from, setFrom] = useState('2004-01-01');
  const [to, setTo] = useState('2004-02-01');
  const [symbol, setSymbol] = useState('AAPL,MSFT,AMZN,AA');
  const onSubmit = ev => {
    ev.preventDefault();
    fetch(`${ZDB_URL}/ohlcv/${table}/${from}/${to}?symbols=${symbol}`)
      .then(res => res.json())
      .then(data => {
        console.log('fetched', Object.keys(data.results).length)
        Object.entries(data.results).forEach(([key, res]) => setTimeout(() => {
          opts.title = key;
          res.t = res.t.map(t => t / 1000000000);
          opts.scales.x.range = [data.min_date / 1000000000, data.max_date / 1000000000];
          console.log('range', opts.scales.x.range)
          opts.scales.vol.range = [0, Math.max(...res.v) * 5];
          res = [
            res.t,
            res.o,
            res.h,
            res.l,
            res.c,
            res.v,
          ];

          const chartContainer = document.createElement('div');
          new uPlot(
            opts,
            res,
            chartContainer
          );
          containerRef.current.appendChild(chartContainer);
        }, 0))
    });
  };

  return (
    <form onSubmit={onSubmit}>
      <fieldset>
        <label>Table</label>
        <input value={table} onChange={e => setTable(e.target.value)} />
        <label>Symbol</label>
        <input value={symbol} onChange={e => setSymbol(e.target.value)} />
        <label>From</label>
        <input value={from} onChange={e => setFrom(e.target.value)} />
        <label>To</label>
        <input value={to} onChange={e => setTo(e.target.value)} />
        <button>
          Submit
        </button>
      </fieldset>
    </form>
  );
}

const App = () => {
  const containerRef = useRef();

  return (
    <Fragment>
      <DataForm containerRef={containerRef} />
      <div className="chart-grid" ref={containerRef} />
    </Fragment>
  );
}

render(<App />, document.body);

