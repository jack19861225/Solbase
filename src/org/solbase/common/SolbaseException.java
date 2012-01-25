package org.solbase.common;

import java.io.CharArrayWriter;
import java.io.PrintWriter;

import org.slf4j.Logger;

public class SolbaseException extends RuntimeException {

	public enum ErrorCode {
		BAD_REQUEST(400), UNAUTHORIZED(401), FORBIDDEN(403), NOT_FOUND(404), SERVER_ERROR(500), SERVICE_UNAVAILABLE(503), UNKNOWN(0);
		final int code;

		private ErrorCode(int c) {
			code = c;
		}

		public static ErrorCode getErrorCode(int c) {
			for (ErrorCode err : values()) {
				if (err.code == c)
					return err;
			}
			return UNKNOWN;
		}
	};

	public boolean logged = false;

	public SolbaseException(ErrorCode code, String msg) {
		super(msg);
		this.code = code.code;
	}

	public SolbaseException(ErrorCode code, String msg, boolean alreadyLogged) {
		super(msg);
		this.code = code.code;
		this.logged = alreadyLogged;
	}

	public SolbaseException(ErrorCode code, String msg, Throwable th, boolean alreadyLogged) {
		super(msg, th);
		this.code = code.code;
		logged = alreadyLogged;
	}

	public SolbaseException(ErrorCode code, String msg, Throwable th) {
		this(code, msg, th, true);
	}

	public SolbaseException(ErrorCode code, Throwable th) {
		super(th);
		this.code = code.code;
		logged = true;
	}

	int code = 0;

	public int code() {
		return code;
	}

	public void log(Logger log) {
		log(log, this);
	}

	public static void log(Logger log, Throwable e) {
		log.error(toStr(e));
		if (e instanceof SolbaseException) {
			((SolbaseException) e).logged = true;
		}
	}

	public static void log(Logger log, String msg, Throwable e) {
		log.error(msg + ':' + toStr(e));
		if (e instanceof SolbaseException) {
			((SolbaseException) e).logged = true;
		}
	}

	public static void logOnce(Logger log, String msg, Throwable e) {
		if (e instanceof SolbaseException) {
			if (((SolbaseException) e).logged)
				return;
		}
		if (msg != null)
			log(log, msg, e);
		else
			log(log, e);
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

	public static String toStr(Throwable e) {
		CharArrayWriter cw = new CharArrayWriter();
		PrintWriter pw = new PrintWriter(cw);
		e.printStackTrace(pw);
		pw.flush();
		return cw.toString();

	}
}