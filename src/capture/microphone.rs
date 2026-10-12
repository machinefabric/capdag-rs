//! microphone — the microphone capture backend (13.2 §Reference Media,
//! device-reference transport resolution).
//!
//! Resolves `media:audio;live;microphone`; delivers
//! `media:audio;audio-frames;pcm` items: raw signed 16-bit little-endian
//! interleaved PCM, one item per decoded capture buffer, with `pts_us`
//! from the device stream's timebase. Capture goes through the vendored
//! ffmpeg's avdevice input formats (`alsa` on Linux, `avfoundation` on
//! macOS).
//!
//! `open` starts a capture thread: it opens the device, decodes packets to
//! frames, converts to s16 interleaved via swresample, and pushes each
//! buffer into the [`LiveFeedSink`] — which applies the feed's overrun
//! policy at the capture edge (12.5 §Overrun). A `push` returning false
//! means the feed closed (stop/drain or abort): release the device and end
//! the thread. A device FAILURE mid-capture fails the feed via
//! `sink.fail` — never a silent short feed.
//!
//! Selector contract:
//! - `device`: the avdevice url (e.g. `default`, `hw:0` on alsa;
//!   `:0` on avfoundation). Defaults to the platform default device.
//! - `params.sample_rate` (default 48000), `params.channels` (default 1):
//!   requested capture format — the format ACTUALS ride the stream meta.
//! - unknown params are a hard error: a misspelled knob must never be
//!   silently ignored.

use crate::bifaci::live_feed::{LiveFeedItem, LiveFeedSelector, LiveFeedSink};
use crate::bifaci::cartridge_runtime::RuntimeError;
use ffmpeg_bundle as ff;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};

const DEFAULT_SAMPLE_RATE: u64 = 48_000;
const DEFAULT_CHANNELS: u64 = 1;

// avdevice + dictionary externs not in ffmpeg-bundle's shared list. The
// symbols exist once the vendored ffmpeg dist is built with
// `--enable-avdevice` (ffmpeg-bundle/scripts/build-ffmpeg.sh).
extern "C" {
    fn avdevice_register_all();
    fn av_find_input_format(short_name: *const c_char) -> *const std::ffi::c_void;
    fn av_dict_set(
        pm: *mut *mut ff::AVDictionary,
        key: *const c_char,
        value: *const c_char,
        flags: c_int,
    ) -> c_int;
    fn av_dict_free(m: *mut *mut ff::AVDictionary);
}

/// The avdevice input-format short name for this platform's microphone.
fn input_format_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "alsa"
    }
    #[cfg(target_os = "macos")]
    {
        "avfoundation"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // No capture backend is built for this platform — opening fails
        // hard below with a clear reason (never a silent empty feed).
        ""
    }
}

/// The platform's default-device url when the selector names none.
fn default_device() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "default"
    }
    #[cfg(target_os = "macos")]
    {
        // avfoundation "audio only, default device".
        ":0"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        ""
    }
}

/// Open the microphone the selector names and start capturing into `sink`.
/// Returns the stream-level format actuals for STREAM_START meta. A device
/// that cannot be opened is a hard error — never a silent empty feed.
pub fn open(
    selector: &LiveFeedSelector,
    sink: LiveFeedSink,
) -> Result<Option<crate::StreamMeta>, RuntimeError> {
    // Params: only the declared knobs; a misspelled one fails hard.
    let mut sample_rate = DEFAULT_SAMPLE_RATE;
    let mut channels = DEFAULT_CHANNELS;
    for (key, value) in &selector.params {
        let as_count = value.as_u64().ok_or_else(|| {
            RuntimeError::Handler(format!(
                "microphone param '{key}' must be a positive integer, got {value}"
            ))
        });
        match key.as_str() {
            "sample_rate" => sample_rate = as_count?,
            "channels" => channels = as_count?,
            other => {
                return Err(RuntimeError::Handler(format!(
                    "microphone capture knows no param '{other}' — known params: \
                     sample_rate, channels"
                )));
            }
        }
    }
    if sample_rate == 0 || channels == 0 {
        return Err(RuntimeError::Handler(
            "microphone sample_rate and channels must be positive".to_string(),
        ));
    }
    let format_name = input_format_name();
    if format_name.is_empty() {
        return Err(RuntimeError::Handler(
            "no microphone capture backend is built for this platform".to_string(),
        ));
    }
    let device = selector
        .device
        .clone()
        .unwrap_or_else(|| default_device().to_string());

    // Open on THIS thread so a bad device fails the resolution, not the
    // capture thread; then hand the opened input to the capture thread.
    let opened = open_capture(&device, format_name, sample_rate, channels)?;
    let meta = opened.stream_meta();

    std::thread::Builder::new()
        .name("mic-capture".to_string())
        .spawn(move || opened.pump(sink))
        .map_err(|e| RuntimeError::Handler(format!("failed to spawn capture thread: {e}")))?;
    Ok(Some(meta))
}

/// An opened capture input: format context + decoder + resampler state.
struct OpenCapture {
    fmt: *mut ff::AVFormatContext,
    dec: *mut ff::AVCodecContext,
    stream_index: c_int,
    time_base: ff::AVRational,
    sample_rate: u64,
    channels: u64,
    swr: *mut ff::SwrContext,
}

// The raw pointers are owned by this struct and only ever used from the
// single capture thread after `open` hands the struct over.
unsafe impl Send for OpenCapture {}

impl OpenCapture {
    fn stream_meta(&self) -> crate::StreamMeta {
        crate::StreamMeta::from([
            (
                "feed".to_string(),
                ciborium::Value::Text("microphone".to_string()),
            ),
            (
                "sample_rate".to_string(),
                ciborium::Value::Integer((self.sample_rate as i64).into()),
            ),
            (
                "channels".to_string(),
                ciborium::Value::Integer((self.channels as i64).into()),
            ),
            (
                "sample_format".to_string(),
                ciborium::Value::Text("s16le".to_string()),
            ),
        ])
    }

    /// Read packets → decode → s16 interleaved → sink, until the sink
    /// closes, a stop condition ends the feed, or the device FAILS — a
    /// failure is delivered through `sink.fail`, never a silent stream end
    /// (a dying device must not masquerade as a short but successful
    /// recording). All loss accounting lives in the sink.
    fn pump(self, sink: LiveFeedSink) {
        unsafe {
            let pkt = ff::av_packet_alloc();
            let frame = ff::av_frame_alloc();
            let out_frame = ff::av_frame_alloc();
            // The resampler is configured from the FIRST decoded frame via
            // `ffmpeg_embed_swr_setup` (which copies the frame's ch_layout
            // verbatim). Letting `swr_convert_frame` auto-configure instead
            // trips its UNSPEC-vs-NATIVE layout comparison and every frame
            // after the first fails with AVERROR_INPUT_CHANGED.
            let mut swr_ready = false;
            loop {
                let rc = ff::av_read_frame(self.fmt, pkt);
                if rc < 0 {
                    if rc == ff::averror_eagain() {
                        // Non-blocking device with no period ready yet.
                        std::thread::sleep(std::time::Duration::from_millis(2));
                        continue;
                    }
                    // Device gone / read error mid-capture: fail the feed
                    // loudly (unless the feed was already closed by a stop).
                    if !sink.is_closed() {
                        sink.fail(format!(
                            "microphone capture read failed: {}",
                            ff::av_strerror_owned(rc)
                        ));
                    }
                    break;
                }
                if ff::ffmpeg_embed_packet_stream_index(pkt) != self.stream_index {
                    ff::av_packet_unref(pkt);
                    continue;
                }
                let pts = ff::ffmpeg_embed_packet_pts(pkt);
                if ff::avcodec_send_packet(self.dec, pkt) >= 0 {
                    while ff::avcodec_receive_frame(self.dec, frame) >= 0 {
                        // Raw-PCM decoder frames can carry an UNSPEC channel
                        // order; promote to the canonical native layout so
                        // swr's per-frame layout comparison is stable (the
                        // same normalization the transcode path applies).
                        let rc = ff::ffmpeg_embed_frame_normalize_ch_layout(frame);
                        if rc < 0 {
                            sink.fail(format!(
                                "microphone frame layout normalization failed: {}",
                                ff::av_strerror_owned(rc)
                            ));
                            self.release(pkt, frame, out_frame);
                            return;
                        }
                        // Convert whatever the device delivers to s16
                        // interleaved at the negotiated rate/channels.
                        ff::av_frame_unref(out_frame);
                        ff::ffmpeg_embed_frame_set_audio_out(
                            out_frame,
                            self.sample_rate as c_int,
                            self.channels as c_int,
                        );
                        if !swr_ready {
                            let rc = ff::ffmpeg_embed_swr_setup(self.swr, frame, out_frame, 0);
                            if rc < 0 {
                                sink.fail(format!(
                                    "microphone resampler setup failed: {}",
                                    ff::av_strerror_owned(rc)
                                ));
                                self.release(pkt, frame, out_frame);
                                return;
                            }
                            swr_ready = true;
                        }
                        let mut rc = ff::swr_convert_frame(self.swr, out_frame, frame);
                        if rc < 0 {
                            // swresample's documented contract: a frame whose
                            // layout/rate/format differs from the configured
                            // shape returns *_CHANGED — reconfigure from the
                            // CURRENT frames and retry. Devices do this once
                            // at warmup (the first decoded frame can carry a
                            // different ch_layout order than the rest).
                            let setup = ff::ffmpeg_embed_swr_setup(self.swr, frame, out_frame, 0);
                            if setup >= 0 {
                                rc = ff::swr_convert_frame(self.swr, out_frame, frame);
                            }
                        }
                        if rc < 0 {
                            sink.fail(format!(
                                "microphone sample conversion failed: {}",
                                ff::av_strerror_owned(rc)
                            ));
                            self.release(pkt, frame, out_frame);
                            return;
                        }
                        let samples = ff::ffmpeg_embed_frame_nb_samples(out_frame) as usize;
                        let bytes = samples * self.channels as usize * 2;
                        let data = ff::ffmpeg_embed_frame_data(out_frame, 0);
                        if data.is_null() || bytes == 0 {
                            // A rate-converting resampler may legitimately
                            // buffer without output this round.
                            ff::av_frame_unref(frame);
                            continue;
                        }
                        let payload = std::slice::from_raw_parts(data, bytes).to_vec();
                        let pts_us = if pts >= 0 {
                            (pts as i128 * self.time_base.num as i128 * 1_000_000
                                / self.time_base.den.max(1) as i128) as u64
                        } else {
                            0
                        };
                        let capture_ts_us = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        let delivered = sink.push(LiveFeedItem {
                            payload,
                            pts_us,
                            capture_ts_us,
                        });
                        ff::av_frame_unref(frame);
                        if !delivered {
                            // Feed closed (stop/drain, abort, or max_items):
                            // release the device and end.
                            self.release(pkt, frame, out_frame);
                            return;
                        }
                    }
                }
                ff::av_packet_unref(pkt);
            }
            self.release(pkt, frame, out_frame);
        }
    }

    unsafe fn release(
        &self,
        mut pkt: *mut ff::AVPacket,
        mut frame: *mut ff::AVFrame,
        mut out_frame: *mut ff::AVFrame,
    ) {
        ff::av_packet_free(&mut pkt);
        ff::av_frame_free(&mut frame);
        ff::av_frame_free(&mut out_frame);
        let mut dec = self.dec;
        ff::avcodec_free_context(&mut dec);
        let mut fmt = self.fmt;
        ff::avformat_close_input(&mut fmt);
        let mut swr = self.swr;
        ff::swr_free(&mut swr);
    }
}

fn open_capture(
    device: &str,
    format_name: &str,
    sample_rate: u64,
    channels: u64,
) -> Result<OpenCapture, RuntimeError> {
    let err = |what: &str, rc: c_int| {
        RuntimeError::Handler(format!(
            "microphone '{device}': {what} failed: {}",
            ff::av_strerror_owned(rc)
        ))
    };
    unsafe {
        avdevice_register_all();
        let format_cstr = CString::new(format_name).expect("static name");
        let input_format = av_find_input_format(format_cstr.as_ptr());
        if input_format.is_null() {
            return Err(RuntimeError::Handler(format!(
                "avdevice input format '{format_name}' is not built into this ffmpeg — \
                 regenerate the ffmpeg-bundle dist with --enable-avdevice"
            )));
        }
        let device_cstr = CString::new(device)
            .map_err(|_| RuntimeError::Handler("device name contains NUL".to_string()))?;

        let mut opts: *mut ff::AVDictionary = std::ptr::null_mut();
        let set = |opts: *mut *mut ff::AVDictionary, k: &str, v: String| {
            let k = CString::new(k).expect("static key");
            let v = CString::new(v).expect("numeric value");
            av_dict_set(opts, k.as_ptr(), v.as_ptr(), 0);
        };
        set(&mut opts, "sample_rate", sample_rate.to_string());
        set(&mut opts, "channels", channels.to_string());

        let mut fmt: *mut ff::AVFormatContext = std::ptr::null_mut();
        let rc = ff::avformat_open_input(
            &mut fmt,
            device_cstr.as_ptr(),
            input_format as *const _,
            &mut opts,
        );
        av_dict_free(&mut opts);
        if rc < 0 {
            return Err(err("open", rc));
        }
        let rc = ff::avformat_find_stream_info(fmt, std::ptr::null_mut());
        if rc < 0 {
            ff::avformat_close_input(&mut { fmt });
            return Err(err("stream info", rc));
        }

        // First audio stream is the capture stream.
        let nb = ff::ffmpeg_embed_format_nb_streams(fmt);
        let mut found: Option<(c_int, *const ff::AVStream)> = None;
        for i in 0..nb {
            let stream = ff::ffmpeg_embed_format_stream(fmt, i);
            let par = ff::ffmpeg_embed_stream_codecpar(stream);
            if ff::ffmpeg_embed_codecpar_codec_type(par) == ff::AVMEDIA_TYPE_AUDIO {
                found = Some((ff::ffmpeg_embed_stream_index(stream), stream));
                break;
            }
        }
        let Some((stream_index, stream)) = found else {
            ff::avformat_close_input(&mut { fmt });
            return Err(RuntimeError::Handler(format!(
                "microphone '{device}': the device exposes no audio stream"
            )));
        };
        let par = ff::ffmpeg_embed_stream_codecpar(stream);
        let codec = ff::avcodec_find_decoder(ff::ffmpeg_embed_codecpar_codec_id(par));
        if codec.is_null() {
            ff::avformat_close_input(&mut { fmt });
            return Err(RuntimeError::Handler(format!(
                "microphone '{device}': no decoder for the device's codec"
            )));
        }
        let dec = ff::avcodec_alloc_context3(codec);
        ff::avcodec_parameters_to_context(dec, par);
        let rc = ff::avcodec_open2(dec, codec, std::ptr::null_mut());
        if rc < 0 {
            ff::avcodec_free_context(&mut { dec });
            ff::avformat_close_input(&mut { fmt });
            return Err(err("decoder open", rc));
        }

        let swr = ff::swr_alloc();
        if swr.is_null() {
            ff::avcodec_free_context(&mut { dec });
            ff::avformat_close_input(&mut { fmt });
            return Err(RuntimeError::Handler(
                "microphone: swresample alloc failed".to_string(),
            ));
        }

        Ok(OpenCapture {
            fmt,
            dec,
            stream_index,
            time_base: ff::ffmpeg_embed_stream_time_base(stream),
            sample_rate,
            channels,
            swr,
        })
    }
}

