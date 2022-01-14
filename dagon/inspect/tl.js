/** @enum {string} */
const EndState = {
  Succeeded: 'succeeded',
  Pending: 'pending',
  Running: 'running',
  Failed: 'failed',
  Cancelled: 'cancelled',
};

const COLOR_TABLE = {
  process: '#3bb',
};

function reduce(iter, init, fn) {
  for (const item of iter) {
    init = fn(init, item);
  }
  return init;
}

function each(iter, fn) {
  reduce(iter, null, (_, item) => fn(item));
}

function* map(iter, fn) {
  for (const item of iter) {
    yield fn(item);
  }
}

function* filter(iter, fn) {
  for (const item of iter) {
    if (fn(item)) {
      yield item;
    }
  }
}

/**
 * @typedef {{
 *  name: string;
 *  start_time: number;
 *  duration: number;
 *  end_state: EndState;
 *  target_id: number;
 *  target_run_id: number;
 * }} TargetItem
 *
 * @typedef {{
 *  cmd: string[];
 *  duration: number;
 *  retc: number;
 *  start_cwd: string;
 *  start_time: number;
 *  stderr: string;
 *  stdout: string;
 * }} ProcessExec
 *
 * @typedef {{
 *  process_id?: number;
 *  user_meta?: any;
 * }} IntervalMeta
 *
 * @typedef {{
 *  label: string;
 *  meta: IntervalMeta;
 *  start_time: number;
 *  duration: number;
 *  target_run_id: number;
 * }} Interval
 *
 * @typedef {{
 *  targets: TargetItem[];
 *  intervals: {[id: number]: Interval};
 *  proc_execs: {[id: number]: ProcessExec};
 *  start_time: number;
 * }} TimelineData
 */

/** @type {TimelineData} */
var TIMELINE_DATA;

class TimeStrip extends HTMLElement {
  constructor() {
    super();
    this.label = '';
    this.startTime = 0;
    this.duration = 0;
  }

  get fullLabel() {
    return `${this.label} - ${this.duration}s`
  }
  /**
   * @param {{
   *  label: string;
   *  startTime: number,
   *  duration: number,
   *  vertOffset: number,
   *  endState: EndState,
   *  color: string,
   * }} param0
   */
  setData({ label, startTime, duration, endState, vertOffset, color }) {
    this.label = label;
    this.startTime = startTime;
    this.duration = duration;
    this.style.setProperty('--tl-strip-start-time-px', `${startTime}px`);
    this.style.setProperty('--tl-strip-duration-px', `${duration}px`);
    this.style.setProperty('--tl-strip-color', color || `var(--tl-strip-color-${endState})`);
    this.style.setProperty('--tl-strip-vert-offset', vertOffset.toString());
    const labelDiv = document.createElement('div');
    labelDiv.classList.add('label');
    labelDiv.innerText = this.fullLabel;
    this.title = labelDiv.innerText;
    this.appendChild(labelDiv);
  }

  connectedCallback() {
    this.addEventListener('mousemove', _ => {
      this.dispatchEvent(new Event('tl-strip-hover', { bubbles: true }));
    });
  }
}


/** @param {Animation} anim */
function waitAnimation(anim) {
  return new Promise(r => {
    anim.addEventListener('cancel', r);
    anim.addEventListener('finish', r);
  });
}


class TargetElement extends HTMLElement {
  constructor() {
    super();
    /** @type {TargetItem} */
    this._target = null;
    /** @type {TimeStrip} */
    this._rootStrip = null;
    /** @type {TimeStrip[]} */
    this._strips = [];
    this._isExpanded = false;
    this.maxHeight = 1;
    this.startTimeOffset = 0;
  }

  /** @param {TargetItem} t */
  setTarget(t, startTimeOffset) {
    this._target = t;
    this.startTimeOffset = startTimeOffset;
    const strip = this._rootStrip = (/** @type {TimeStrip} */ document.createElement('tl-strip'));
    strip.setData({
      label: t.name,
      startTime: t.start_time - startTimeOffset,
      duration: t.duration,
      endState: t.end_state,
      vertOffset: 0,
    });
    this.appendChild(strip);
    if (this.intervals.length) {
      strip.classList.add('clickable');
      strip.addEventListener('click', ev => {
        this._isExpanded = !this._isExpanded;
        this._isExpanded ? this._showIntervals() : this._hideIntervals();
      });
    }
    this._strips.push(strip);
  }

  _fireStripsChanged() {
    this.dispatchEvent(new Event('tl-strips-changed', { bubbles: true }));
  }

  _hideIntervals() {
    const childStrips = filter(this._strips, s => s !== this._rootStrip);
    each(childStrips, el => {
      const anim = el.animate([
        {
          opacity: 1,
        },
        {
          opacity: 0,
        }
      ], {
        duration: 200,
      });
      waitAnimation(anim).then(el.remove.bind(el));
    });
    this._strips = [this._rootStrip];
    this.maxHeight = 1;
    this._fireStripsChanged();
  }

  get intervals() {
    return TIMELINE_DATA.intervals[this._target.target_run_id] || []
  }

  _showIntervals() {
    /** @type {Interval[]} */
    const intervals = this.intervals;
    let timeStack = [];
    let maxHeight = 1;
    for (const int of intervals) {
      /** @type {TimeStrip} */
      const strip = document.createElement('tl-strip');
      while (timeStack.length && timeStack[timeStack.length - 1] < int.start_time) {
        timeStack.pop();
      }
      const intEndTime = int.start_time + int.duration;
      const randColorNum = Math.floor(Math.random() * 11);
      const randColor = `var(--tl-strip-color-rand-${randColorNum})`;
      timeStack.push(intEndTime);
      strip.setData({
        label: int.label,
        duration: int.duration,
        startTime: int.start_time - this.startTimeOffset,
        endState: EndState.Running,
        vertOffset: timeStack.length,
        color: randColor,
      });
      maxHeight = Math.max(maxHeight, timeStack.length);
      this.appendChild(strip);
      this._strips.push(strip);

      strip.animate([
        {
          opacity: 0,
        },
        {
          opacity: 1
        }
      ], {
        duration: 200,
      });

      let metaData = null;
      try {
        metaData = JSON.parse(int.meta);
      } catch (e) { }

      // Check if this is a process interval
      if (metaData && metaData.process_id !== undefined) {
        const proc = TIMELINE_DATA.proc_execs[parseInt(metaData.process_id)];
        strip.style.setProperty(
          '--tl-strip-color',
          proc.retc === 0
            ? 'var(--tl-strip-color-process-succeeded)'
            : 'var(--tl-strip-color-process-failed)'
        );
        strip.classList.add('clickable');
        strip.addEventListener('click', _ => {
          window.open(
            `${location.href}?show_process=${metaData.process_id}`,
            '_blank',
            'width=800,height=600',
          );
        });
      }
    }
    this.maxHeight = maxHeight + 1;
    this._fireStripsChanged();
  }
}

const TRACK_HEIGHT_COUNT_KEY = '--tl-track-height-count'

class TimelineTrack extends HTMLElement {
  constructor() {
    super();
    /** @type {TargetElement[]} */
    this._targets = []
  }

  connectedCallback() {
    this.addEventListener('tl-strips-changed', _ => {
      const targetHeights = map(this._targets, t => t.maxHeight);
      const maxHeight = reduce(targetHeights, 0, Math.max);

      const prevHeight = getComputedStyle(this).height;
      this.style.setProperty(TRACK_HEIGHT_COUNT_KEY, maxHeight.toString());
      const newHeight = getComputedStyle(this).height;

      const anim = this.animate([
        {
          height: prevHeight,
        },
        {
          height: newHeight,
        }
      ], {
        duration: 200,
      });
      waitAnimation(anim).then(() => {
        this.style.setProperty(TRACK_HEIGHT_COUNT_KEY, maxHeight.toString());
      })
      this.style.setProperty(TRACK_HEIGHT_COUNT_KEY, null);
    });
  }

  /** @param {TargetItem} target */
  addTarget(target, startTimeOffset) {
    /** @type {TargetElement} */
    const targetEl = document.createElement('tl-target');
    this._targets.push(targetEl);
    targetEl.setTarget(target, startTimeOffset);
    this.appendChild(targetEl);
    this.style.setProperty(TRACK_HEIGHT_COUNT_KEY, '1');
  }
}

class Timeline extends HTMLElement {
  constructor() {
    super();
    this._zoom = 1;
    this.zoom = 1;
    /** @type {TimelineData} */
    this._data = null;
  }

  get zoom() {
    return this._zoom;
  }
  set zoom(z) {
    this._zoom = z;
    this.style.setProperty('--hscale', this.zoom);
  }

  connectedCallback() {
    this.addEventListener('wheel', ev => {
      if (ev.shiftKey || ev.altKey) {
        return;
      }
      ev.preventDefault();

      const fac = ev.deltaY < 0 ? 1.5 : 1 / 1.5;

      // Math to help up center on the mouse cursor
      const viewLeft = this.scrollLeft;
      const mouseX = viewLeft + (ev.clientX - this.getBoundingClientRect().left);
      const mouseOffset = mouseX - viewLeft;
      const newMouseX = mouseX * fac;
      const newLeft = newMouseX - mouseOffset;

      // Zoom in/out
      this.zoom *= fac;
      // Center the scroll action on the cursor
      this.scrollLeft = newLeft;
    });
  }

  _load() {
    const runStartTime = this._data.start_time;
    this.style.setProperty('--tl-start-time-px', `${runStartTime}px`);
    let maxTime = 0;
    let maxThread = 0;
    /** @type {TimelineTrack[]} */
    const tracks = [];
    /** @type {TargetElement[]} */
    const targets = [];
    const tracksDiv = this.querySelector('#tracks-container');
    for (const target of this._data.targets) {
      maxThread = Math.max(maxThread, target.target_run_id);
      maxTime = Math.max(maxTime, target.start_time + target.duration);
      // Create a new track, if necessary
      while (target.target_run_id > tracks.length) {
        const newTrack = document.createElement('tl-timeline-track');
        tracksDiv.appendChild(newTrack);
        tracks.push(newTrack);
      }
      const track = tracks[target.target_run_id - 1];
      track.addTarget(target, runStartTime);
    }
    this.style.setProperty('--tl-max-time-px', (maxTime - runStartTime) + 'px');
    const visibleTime = this.offsetWidth;
    const initHScale = visibleTime / (maxTime - runStartTime);
    this.zoom = initHScale * 0.8;
  }

  /**
   * @param {TimelineData} data
   */
  setData(data) {
    console.assert(this._data === null);
    this._data = data;
    this._load();
  }
}

class Application extends HTMLElement {
  /**
   * @param {string} q
   * @returns {HTMLElement|undefined}
   */
  $(q) {
    return this.querySelector(q);
  }

  /** @returns {Timeline} */
  get timeline() {
    return this.querySelector('tl-timeline');
  }

  _adjustTimeCursor(mouseX) {
    const tl = this.timeline;
    let innerX = mouseX - tl.getBoundingClientRect().x + tl.scrollLeft;
    innerX = Math.min(innerX, tl.scrollLeft + tl.offsetWidth - 5);
    const cursorBar = this.querySelector('#time-cursor');
    cursorBar.classList.toggle('on-right', innerX > tl.offsetWidth / 2);
    cursorBar.style.transform = `translateX(${innerX}px)`;

    // Create a time label
    let seconds = innerX / tl.zoom;
    const secondsPerMinute = 60;
    const secondsPerHour = secondsPerMinute * 60;
    const hours = Math.floor(seconds / secondsPerHour);
    seconds -= (hours * secondsPerHour);
    const minutes = Math.floor(seconds / secondsPerMinute);
    seconds -= (minutes * secondsPerMinute);
    let timeText = '';
    if (hours) { timeText += `${hours}h`; }
    if (minutes || hours) { timeText += `${minutes}m`; }
    seconds = +seconds.toFixed(2);
    timeText += `${seconds}s`;

    const label = cursorBar.querySelector('.label');
    label.innerText = timeText;
  }

  connectedCallback() {
    const vZoomFac = 1.5;
    const hZoomFac = vZoomFac;
    this.$('.controls .zoom.in').addEventListener('click', () => {
      this.timeline.zoom *= 1.5;
    });
    this.$('.controls .zoom.out').addEventListener('click', () => {
      this.timeline.zoom /= 1.5;
    });
    this.addEventListener('mousemove', mouse => {
      this._adjustTimeCursor(mouse.clientX);
    });
    this.addEventListener('wheel', ev => {
      this._adjustTimeCursor(ev.clientX);
    })
    this.addEventListener('tl-strip-hover', ev => {
      /** @type {TimeStrip} */
      const strip = ev.target;
      this.$('#status-bar').innerText = strip.fullLabel;
    });
  }
}

function escapeArg(str) {
  if (str == '') {
    return '""';
  }
  if (/[^\w@%\-+=:,./|]/.test(str)) {
    str = str.replace(/"/g, '\\"');
    return `"${str}"`;
  } else {
    return str;
  }
}

function* matchAll(str, re) {
  let match;
  while ((match = re.exec(str)) !== null) {
    yield match;
  }
}

/**
 * @param {string} output
 */
function* generateTerminalElements(output) {
  const ansiRegex = /(?:\x1b(?:\[|\()([0-?]*[ -/]*)([@-~]))|([^\x1b]+)/g;

  const lineNoProto = document.createElement('div');
  lineNoProto.classList.add('lno');

  let lineNo = 1;

  const lineProto = document.createElement('div');
  lineProto.classList.add('term-line');
  const partSpan = document.createElement('span');
  const cl = partSpan.classList;

  const addPart = (s) => {
    if (s.length) {
      partSpan.innerText = s;
      lineProto.appendChild(partSpan.cloneNode(true));
    }
  };

  const matchesIter = matchAll(output, ansiRegex);

  for (const [full, paramsString, opcode] of matchesIter) {
    switch (opcode) {
      case undefined: {
        console.log('Text item', full);
        if (full.length) {
          if (!full.includes('\n')) {
            addPart(full);
          } else {
            const parts = [...full.split(/\r?\n/g)];
            const lastPart = parts.pop();
            for (const part of parts) {
              addPart(part);

              lineNoProto.innerText = (lineNo++).toString();
              yield lineNoProto.cloneNode(true);

              yield lineProto.cloneNode(true);
              lineProto.innerHTML = '';
            }

            addPart(lastPart);
          }
        }
        break;
      }
      case 'm': {
        for (const param of paramsString.split(/;/g)) {
          console.log('Param', param);
          switch (param) {
            case '0':
              partSpan.className = 'term-line'; break;
            case '1':
              cl.add('bold'); break;
            case '3':
              cl.add('italic'); break;
            case '4':
              cl.add('underline'); break;
            case '21':
            case '22':
              cl.remove('bold', 'italic'); break;
            case '24':
              cl.remove('underline'); break;

            case '90': cl.add('bold');
            case '30':
              cl.add('black'); break;
            case '91': cl.add('bold');
            case '31':
              cl.add('red'); break;
            case '92': cl.add('bold');
            case '32':
              cl.add('green'); break;
            case '93': cl.add('bold');
            case '33':
              cl.add('yellow'); break;
            case '94': cl.add('bold');
            case '34':
              cl.add('blue'); break;
            case '95': cl.add('bold');
            case '35':
              cl.add('magenta'); break;
            case '96': cl.add('bold');
            case '36':
              cl.add('cyan'); break;
            case '97': cl.add('bold');
            case '37':
              cl.add('white'); break;

            case '39':
              cl.remove('black', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan', 'white', 'bold');
              break;
          }
        }
        break;
      }
    }
  }
}

class ProcessView extends HTMLElement {
  constructor() {
    super();
    /** @type {ProcessExec} */
    this._proc = null;
    this._allOutput = '';
  }

  connectedCallback() {
    /** @type {HTMLInputElement} */
    const wordWrap = this.querySelector('#word-wrap');
    wordWrap.addEventListener('change', _ => {
      this.toggleAttribute('wrap-term-output', wordWrap.checked);
    });
    const wordWrapDiv = this.querySelector('#word-wrap-container');
    wordWrapDiv.addEventListener('click', ev => {
      if (ev.target !== wordWrap) {
        wordWrap.click();
      }
    });
  }

  /** @param {ProcessExec} proc */
  loadProcess(proc) {
    this._proc = proc;

    const argv = JSON.parse(proc.cmd);

    const cmd = this.querySelector('div.cmd');

    const space = document.createElement('span');
    space.classList.add('space');
    space.innerText = ' ';
    const argSp = document.createElement('span');
    argSp.classList.add('arg');

    for (const str of argv.map(escapeArg)) {
      argSp.innerText = str;
      cmd.appendChild(argSp.cloneNode(true));
      cmd.appendChild(space.cloneNode(true));
    }

    this.querySelector('div.cwd').innerText = proc.start_cwd;
    this.querySelector('div.rc').innerText = proc.retc.toString();
    const term = this.querySelector('div.output');
    each(
      generateTerminalElements(atob(proc.stdout)),
      term.appendChild.bind(term),
    );
  }
}

async function mainApp() {
  /** @type {HTMLTemplateElement} */
  const appTemplate = document.getElementById('app-template');
  const frag = document.importNode(appTemplate.content, true);

  /** @type {Application} */
  const app = frag.firstElementChild;
  document.body.appendChild(frag);

  app.timeline.setData(TIMELINE_DATA);

  const startTime = new Date(TIMELINE_DATA.start_time * 1000);
  document.title = `Dagon Timeline - ${startTime.toLocaleDateString()} ${startTime.toLocaleTimeString()}`;
}

/**
 * @param {number} pid
 */
async function showProcApp(pid) {
  /** @type {HTMLTemplateElement} */
  const showProcTemplate = document.getElementById('show-proc-template');
  const frag = document.importNode(showProcTemplate.content, true);

  /** @type {ProcessView} */
  const view = frag.firstElementChild;
  document.body.appendChild(frag);

  const proc = TIMELINE_DATA.proc_execs[pid];
  view.loadProcess(proc);
  document.title = 'Subprocess view';
}

async function main() {
  await customElements.define('tl-target', TargetElement);
  await customElements.define('tl-strip', TimeStrip);
  await customElements.define('tl-timeline-track', TimelineTrack);
  await customElements.define('tl-timeline', Timeline);
  await customElements.define('tl-app', Application);
  await customElements.define('tl-proc-view', ProcessView);

  const currentURL = new URL(location.href);
  const showProc = currentURL.searchParams.get('show_process')
  if (showProc) {
    await showProcApp(parseInt(showProc));
  } else {
    await mainApp();
  }
}

main().catch(e => {
  console.error(e);
  debugger;
});
