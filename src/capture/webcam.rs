//! webcam — the camera capture backend (13.2 §Reference Media,
//! device-reference transport resolution).
//!
//! Resolves `media:image;live;webcam`; delivers `media:image;video-frame`
//! items: one PNG-encoded frame per item, with `pts_us` from the device
//! stream's timebase. Capture goes through the vendored ffmpeg's avdevice
//! input formats (`v4l2` on Linux, `avfoundation` on macOS).
//!
//! `open` starts a capture thread: it opens the device, decodes packets to
//! frames, converts to RGBA via swscale, encodes PNG, and pushes each
//! frame into the [`LiveFeedSink`] — which applies the feed's overrun
//! policy at the capture edge (12.5 §Overrun). A `push` returning false
//! means the feed closed (stop/drain or abort): release the device and end
//! the thread. A device FAILURE mid-capture fails the feed via
//! `sink.fail` — never a silent short feed.
//!
//! Selector contract:
//! - `device`: the avdevice url (e.g. `/dev/video0` on v4l2; `0` on
//!   avfoundation). Defaults to the platform default device.
//! - `params.width` / `params.height` (requested capture size) and
//!   `params.framerate` (requested rate). All optional — the device's
//!   defaults apply; the format ACTUALS ride the stream meta.
//! - unknown params are a hard error: a misspelled knob must never be
//!   silently ignored.

use anyhow::Result;
use crate::bifaci::cartridge_runtime::RuntimeError;
use crate::bifaci::live_feed::{LiveFeedItem, LiveFeedSelector, LiveFeedSink};
use ffmpeg_bundle as ff;
use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

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

/// The avdevice input-format short name for this platform's camera.
fn input_format_name() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "v4l2"
    }
    #[cfg(target_os = "macos")]
    {
        "avfoundation"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        ""
    }
}

/// The platform's default-device url when the selector names none.
fn default_device() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "/dev/video0"
    }
    #[cfg(target_os = "macos")]
    {
        // avfoundation "default video device".
        "0"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        ""
    }
}

/// The frame rate asked for when the selector names none.
///
/// avfoundation's own default is NTSC's 29.97, a broadcast rate that Mac
/// cameras do not offer: a FaceTime HD camera offers 15 to 30 at every size,
/// so a capture that named no rate was refused, and the refusal reached the
/// caller as nothing more than "Input/output error". 30 is the rate cameras
/// on macOS offer. v4l2 opens a device at a rate the device itself chooses.
fn default_framerate() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        Some(30)
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

/// Open the camera the selector names and start capturing into `sink`.
/// Returns the stream-level format actuals for STREAM_START meta. A device
/// that cannot be opened is a hard error — never a silent empty feed.
pub fn open(
    selector: &LiveFeedSelector,
    sink: LiveFeedSink,
) -> Result<Option<crate::StreamMeta>, RuntimeError> {
    // Params: only the declared knobs; a misspelled one fails hard.
    let mut width: Option<u64> = None;
    let mut height: Option<u64> = None;
    let mut framerate: Option<u64> = None;
    for (key, value) in &selector.params {
        let as_count = value.as_u64().ok_or_else(|| {
            RuntimeError::Handler(format!(
                "webcam param '{key}' must be a positive integer, got {value}"
            ))
        });
        match key.as_str() {
            "width" => width = Some(as_count?),
            "height" => height = Some(as_count?),
            "framerate" => framerate = Some(as_count?),
            other => {
                return Err(RuntimeError::Handler(format!(
                    "webcam capture knows no param '{other}' — known params: \
                     width, height, framerate"
                )));
            }
        }
    }
    if width.is_some() != height.is_some() {
        return Err(RuntimeError::Handler(
            "webcam width and height must be given together".to_string(),
        ));
    }
    let format_name = input_format_name();
    if format_name.is_empty() {
        return Err(RuntimeError::Handler(
            "no webcam capture backend is built for this platform".to_string(),
        ));
    }
    let device = selector
        .device
        .clone()
        .unwrap_or_else(|| default_device().to_string());
    let framerate = framerate.or_else(default_framerate);

    // Open on THIS thread so a bad device fails the resolution, not the
    // capture thread; then hand the opened input to the capture thread.
    let opened = open_capture(&device, format_name, width, height, framerate)?;
    let meta = opened.stream_meta();

    std::thread::Builder::new()
        .name("webcam-capture".to_string())
        .spawn(move || opened.pump(sink))
        .map_err(|e| RuntimeError::Handler(format!("failed to spawn capture thread: {e}")))?;
    Ok(Some(meta))
}

/// An opened capture input: format context + decoder state. The swscale
/// context is built lazily on the first decoded frame (its pixel format
/// is only known then), exactly like `extract_frames`.
struct OpenCapture {
    fmt: *mut ff::AVFormatContext,
    dec: *mut ff::AVCodecContext,
    stream_index: c_int,
    time_base: ff::AVRational,
    width: c_int,
    height: c_int,
}

// The raw pointers are owned by this struct and only ever used from the
// single capture thread after `open` hands the struct over.
unsafe impl Send for OpenCapture {}

impl OpenCapture {
    fn stream_meta(&self) -> crate::StreamMeta {
        crate::StreamMeta::from([
            (
                "feed".to_string(),
                ciborium::Value::Text("webcam".to_string()),
            ),
            (
                "width".to_string(),
                ciborium::Value::Integer((self.width as i64).into()),
            ),
            (
                "height".to_string(),
                ciborium::Value::Integer((self.height as i64).into()),
            ),
            (
                "frame_format".to_string(),
                ciborium::Value::Text("png".to_string()),
            ),
        ])
    }

    /// Read packets → decode → RGBA → PNG → sink, until the sink closes, a
    /// stop condition ends the feed, or the device FAILS — a failure is
    /// delivered through `sink.fail`, never a silent stream end (a dying
    /// device must not masquerade as a short but successful capture). All
    /// loss accounting lives in the sink.
    fn pump(self, sink: LiveFeedSink) {
        unsafe {
            let pkt = ff::av_packet_alloc();
            let frame = ff::av_frame_alloc();
            let mut sws: *mut ff::SwsContext = ptr::null_mut();
            let mut sws_src: (c_int, c_int, c_int) = (0, 0, -1);
            let mut rgba_buf: Vec<u8> = Vec::new();
            loop {
                let rc = ff::av_read_frame(self.fmt, pkt);
                if rc < 0 {
                    if rc == ff::averror_eagain() {
                        // Non-blocking device with no frame ready yet.
                        std::thread::sleep(std::time::Duration::from_millis(2));
                        continue;
                    }
                    // Device gone / read error mid-capture: fail the feed
                    // loudly (unless the feed was already closed by a stop).
                    if !sink.is_closed() {
                        sink.fail(format!(
                            "webcam capture read failed: {}",
                            ff::av_strerror_owned(rc)
                        ));
                    }
                    break;
                }
                if ff::ffmpeg_embed_packet_stream_index(pkt) != self.stream_index {
                    ff::av_packet_unref(pkt);
                    continue;
                }
                if ff::avcodec_send_packet(self.dec, pkt) >= 0 {
                    while ff::avcodec_receive_frame(self.dec, frame) >= 0 {
                        let w = ff::ffmpeg_embed_frame_width(frame);
                        let h = ff::ffmpeg_embed_frame_height(frame);
                        let pix_fmt = ff::ffmpeg_embed_frame_pix_fmt(frame);
                        if w <= 0 || h <= 0 {
                            ff::av_frame_unref(frame);
                            continue;
                        }
                        if sws.is_null() || sws_src != (w, h, pix_fmt) {
                            if !sws.is_null() {
                                ff::sws_freeContext(sws);
                            }
                            sws = ff::sws_getContext(
                                w,
                                h,
                                pix_fmt,
                                w,
                                h,
                                ff::AV_PIX_FMT_RGBA,
                                ff::SWS_BILINEAR,
                                ptr::null_mut(),
                                ptr::null_mut(),
                                ptr::null(),
                            );
                            if sws.is_null() {
                                // A pixel format swscale cannot convert is a
                                // device we cannot serve — fail the feed.
                                sink.fail(format!(
                                    "webcam pixel format {pix_fmt} ({w}x{h}) has no \
                                     swscale conversion to RGBA"
                                ));
                                ff::av_frame_unref(frame);
                                self.release(pkt, frame, sws);
                                return;
                            }
                            sws_src = (w, h, pix_fmt);
                            rgba_buf.resize((w * h * 4) as usize, 0u8);
                        }
                        let src_data: [*const u8; 4] = [
                            ff::ffmpeg_embed_frame_data(frame, 0),
                            ff::ffmpeg_embed_frame_data(frame, 1),
                            ff::ffmpeg_embed_frame_data(frame, 2),
                            ff::ffmpeg_embed_frame_data(frame, 3),
                        ];
                        let src_stride: [c_int; 4] = [
                            ff::ffmpeg_embed_frame_linesize(frame, 0),
                            ff::ffmpeg_embed_frame_linesize(frame, 1),
                            ff::ffmpeg_embed_frame_linesize(frame, 2),
                            ff::ffmpeg_embed_frame_linesize(frame, 3),
                        ];
                        let dst_data: [*mut u8; 4] =
                            [rgba_buf.as_mut_ptr(), ptr::null_mut(), ptr::null_mut(), ptr::null_mut()];
                        let dst_stride: [c_int; 4] = [w * 4, 0, 0, 0];
                        let scaled = ff::sws_scale(
                            sws,
                            src_data.as_ptr(),
                            src_stride.as_ptr(),
                            0,
                            h,
                            dst_data.as_ptr(),
                            dst_stride.as_ptr(),
                        );
                        if scaled != h {
                            sink.fail(format!(
                                "webcam frame scale produced {scaled} of {h} rows — \
                                 swscale failed mid-frame"
                            ));
                            ff::av_frame_unref(frame);
                            self.release(pkt, frame, sws);
                            return;
                        }
                        let png = match encode_rgba_png(w as u32, h as u32, &rgba_buf) {
                            Ok(png) => png,
                            Err(e) => {
                                sink.fail(format!("webcam frame PNG encode failed: {e}"));
                                ff::av_frame_unref(frame);
                                self.release(pkt, frame, sws);
                                return;
                            }
                        };
                        let pts_raw = ff::ffmpeg_embed_frame_pts(frame);
                        let pts_us = if pts_raw != i64::MIN
                            && self.time_base.num > 0
                            && self.time_base.den > 0
                        {
                            (pts_raw as i128 * self.time_base.num as i128 * 1_000_000
                                / self.time_base.den as i128) as u64
                        } else {
                            0
                        };
                        let capture_ts_us = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as u64)
                            .unwrap_or(0);
                        let delivered = sink.push(LiveFeedItem {
                            payload: png,
                            pts_us,
                            capture_ts_us,
                        });
                        ff::av_frame_unref(frame);
                        if !delivered {
                            // Feed closed (stop/drain, abort, or max_items):
                            // release the device and end.
                            self.release(pkt, frame, sws);
                            return;
                        }
                    }
                }
                ff::av_packet_unref(pkt);
            }
            self.release(pkt, frame, sws);
        }
    }

    unsafe fn release(
        &self,
        mut pkt: *mut ff::AVPacket,
        mut frame: *mut ff::AVFrame,
        sws: *mut ff::SwsContext,
    ) {
        ff::av_packet_free(&mut pkt);
        ff::av_frame_free(&mut frame);
        if !sws.is_null() {
            ff::sws_freeContext(sws);
        }
        let mut dec = self.dec;
        ff::avcodec_free_context(&mut dec);
        let mut fmt = self.fmt;
        ff::avformat_close_input(&mut fmt);
    }
}

/// Tightly-packed RGBA8888 → PNG (the same encoding contract as
/// `extract-frames`' PNG items).
fn encode_rgba_png(width: u32, height: u32, rgba: &[u8]) -> Result<Vec<u8>> {
    use image::{codecs::png::PngEncoder, ExtendedColorType, ImageEncoder};
    let mut out = Vec::new();
    PngEncoder::new(&mut out).write_image(rgba, width, height, ExtendedColorType::Rgba8)?;
    Ok(out)
}

fn open_capture(
    device: &str,
    format_name: &str,
    width: Option<u64>,
    height: Option<u64>,
    framerate: Option<u64>,
) -> Result<OpenCapture, RuntimeError> {
    let err = |what: &str, rc: c_int| {
        RuntimeError::Handler(format!(
            "webcam '{device}': {what} failed: {}",
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

        let mut opts: *mut ff::AVDictionary = ptr::null_mut();
        let set = |opts: *mut *mut ff::AVDictionary, k: &str, v: String| {
            let k = CString::new(k).expect("static key");
            let v = CString::new(v).expect("numeric value");
            av_dict_set(opts, k.as_ptr(), v.as_ptr(), 0);
        };
        if let (Some(w), Some(h)) = (width, height) {
            set(&mut opts, "video_size", format!("{w}x{h}"));
        }
        if let Some(rate) = framerate {
            set(&mut opts, "framerate", rate.to_string());
        }

        let mut fmt: *mut ff::AVFormatContext = ptr::null_mut();
        let rc = ff::avformat_open_input(
            &mut fmt,
            device_cstr.as_ptr(),
            input_format as *const _,
            &mut opts,
        );
        av_dict_free(&mut opts);
        if rc < 0 {
            // A device that refuses the mode it was asked for says only that
            // the open failed; what was asked for is said here, so the error
            // names what to change.
            let size = match (width, height) {
                (Some(w), Some(h)) => format!("{w}x{h}"),
                _ => "the device's own size".to_string(),
            };
            let rate = framerate.map_or("the device's own rate".to_string(), |r| format!("{r} fps"));
            return Err(RuntimeError::Handler(format!(
                "webcam '{device}': open failed: {} (asked for {size} at {rate}; a device \
                 that does not offer that mode is refused — set the width, height and \
                 framerate params to one it does)",
                ff::av_strerror_owned(rc)
            )));
        }
        let rc = ff::avformat_find_stream_info(fmt, ptr::null_mut());
        if rc < 0 {
            ff::avformat_close_input(&mut { fmt });
            return Err(err("stream info", rc));
        }

        // First video stream is the capture stream.
        let nb = ff::ffmpeg_embed_format_nb_streams(fmt);
        let mut found: Option<(c_int, *const ff::AVStream)> = None;
        for i in 0..nb {
            let stream = ff::ffmpeg_embed_format_stream(fmt, i);
            let par = ff::ffmpeg_embed_stream_codecpar(stream);
            if ff::ffmpeg_embed_codecpar_codec_type(par) == ff::AVMEDIA_TYPE_VIDEO {
                found = Some((ff::ffmpeg_embed_stream_index(stream), stream));
                break;
            }
        }
        let Some((stream_index, stream)) = found else {
            ff::avformat_close_input(&mut { fmt });
            return Err(RuntimeError::Handler(format!(
                "webcam '{device}': the device exposes no video stream"
            )));
        };
        let par = ff::ffmpeg_embed_stream_codecpar(stream);
        let codec = ff::avcodec_find_decoder(ff::ffmpeg_embed_codecpar_codec_id(par));
        if codec.is_null() {
            ff::avformat_close_input(&mut { fmt });
            return Err(RuntimeError::Handler(format!(
                "webcam '{device}': no decoder for the device's codec"
            )));
        }
        let dec = ff::avcodec_alloc_context3(codec);
        ff::avcodec_parameters_to_context(dec, par);
        let rc = ff::avcodec_open2(dec, codec, ptr::null_mut());
        if rc < 0 {
            ff::avcodec_free_context(&mut { dec });
            ff::avformat_close_input(&mut { fmt });
            return Err(err("decoder open", rc));
        }

        Ok(OpenCapture {
            fmt,
            dec,
            stream_index,
            time_base: ff::ffmpeg_embed_stream_time_base(stream),
            width: ff::ffmpeg_embed_codecpar_width(par),
            height: ff::ffmpeg_embed_codecpar_height(par),
        })
    }
}
