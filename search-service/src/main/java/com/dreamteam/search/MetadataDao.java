package com.dreamteam.search;

import com.dreamteam.search.models.Book;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple SQLite DAO for book metadata.
 * Exposes batch enrichment + optional author/language filtering.
 */
public class MetadataDao implements AutoCloseable {
    private Connection conn;
    private String jdbcUrl;

    public MetadataDao(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        connect();
    }

    private void connect() {
        try { this.conn = DriverManager.getConnection(jdbcUrl); }
        catch (SQLException e) { throw new RuntimeException("Cannot connect to SQLite: " + jdbcUrl, e); }
    }

    public void reload(String jdbcUrl) {
        close();
        this.jdbcUrl = jdbcUrl;
        connect();
    }

    public Book getBookById(int id) {
        String sql = "SELECT book_id, title, author, language FROM books WHERE book_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new Book(
                        rs.getInt("book_id"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("language")
                );
            }
        } catch (SQLException e) { throw new RuntimeException(e); }
    }

    public List<SearchEngine.ScoredDoc> enrichAndFilter(List<SearchEngine.ScoredDoc> docs,
                                                        String authorFilter,
                                                        String languageFilter) {
        if (docs.isEmpty()) return docs;
        String inClause = docs.stream().map(d -> "?").collect(Collectors.joining(","));
        String sql = "SELECT book_id, title, author, language FROM books WHERE book_id IN (" + inClause + ")";
        Map<Integer, Book> byId = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            for (var d : docs) ps.setInt(i++, d.bookId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byId.put(rs.getInt("book_id"), new Book(
                            rs.getInt("book_id"),
                            rs.getString("title"),
                            rs.getString("author"),
                            rs.getString("language")
                    ));
                }
            }
        } catch (SQLException e) { throw new RuntimeException(e); }

        return docs.stream()
                .filter(d -> {
                    Book b = byId.get(d.bookId);
                    if (b == null) return false;
                    boolean ok = true;
                    if (authorFilter != null && !authorFilter.isBlank()) {
                        ok &= b.author() != null &&
                              b.author().toLowerCase().contains(authorFilter.toLowerCase());
                    }
                    if (languageFilter != null && !languageFilter.isBlank()) {
                        ok &= b.language() != null &&
                              b.language().toLowerCase().startsWith(languageFilter.toLowerCase());
                    }
                    return ok;
                })
                .map(d -> new SearchEngine.ScoredDoc(d.bookId, d.score))
                .toList();
    }

    @Override public void close() {
        try { if (conn != null) conn.close(); } catch (SQLException ignored) {}
    }
}
