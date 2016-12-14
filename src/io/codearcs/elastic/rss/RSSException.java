package io.codearcs.elastic.rss;


public class RSSException extends Exception {

	private static final long serialVersionUID = 1L;


	public RSSException( String msg ) {
		super( msg );
	}


	public RSSException( String msg, Throwable error ) {
		super( msg, error );
	}

}
