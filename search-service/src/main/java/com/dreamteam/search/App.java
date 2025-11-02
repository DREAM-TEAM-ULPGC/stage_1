package com.dreamteam.search;

import com.dreamteam.search.util.Config;
import io.javalin.Javalin;
import com.google.gson.Gson;
import java.nio.file.Path;

public class App {
    public static void main(String[] args) {
        String indexPath = Config.getEnvOrDefault("INDEX_PATH", "index/inverted_index.json");
        String dbPath = Config.getEnvOrDefault("DB_PATH", "datamart/datamart.db");
        int port = Integer.parseInt(Config.getEnvOrDefault("PORT", "8082"));

        SearchEngine engine = new SearchEngine(Path.of(indexPath));
        MetadataDao metadataDao = new MetadataDao("jdbc:sqlite:" + dbPath);

        Javalin app = Javalin.create(conf -> conf.http.defaultContentType = "application/json").start(port);
        Gson gson = new Gson();

        app.get("/health", ctx -> ctx.json(new Health("ok", "search", "1.0.0")));

        app.get("/book/{id}", ctx -> {
            try {
                int id = Integer.parseInt(ctx.pathParam("id"));
                var book = metadataDao.getBookById(id);
                if (book == null) ctx.status(404).result("{\"error\":\"book not found\"}");
                else ctx.json(book);
            } catch (Exception e) {
                ctx.status(400).result("{\"error\":\"invalid id\"}");
            }
        });

        app.get("/search", ctx -> {
            String q = ctx.queryParam("q");
            if (q == null || q.isBlank()) {
                ctx.status(400).result("{\"error\":\"missing query param 'q'\"}");
                return;
            }
            String mode = ctx.queryParam("mode", "and");
            String author = ctx.queryParam("author");
            String language = ctx.queryParam("language");
            int page = Integer.parseInt(ctx.queryParam("page", "1"));
            int pageSize = Integer.parseInt(ctx.queryParam("pageSize", "20"));

            var results = engine.search(q, mode);
            var enriched = metadataDao.enrichAndFilter(results, author, language);

            int total = enriched.size();
            int from = Math.max(0, (page - 1) * pageSize);
            int to = Math.min(total, from + pageSize);
            var pageItems = enriched.subList(from, to);

            var response = new SearchResponse(q, mode, page, pageSize, total, pageItems);
            ctx.json(response);
        });

        app.post("/admin/reload", ctx -> {
            try {
                engine.reload(Path.of(indexPath));
                metadataDao.reload("jdbc:sqlite:" + dbPath);
                ctx.json(new Msg("reloaded"));
            } catch (Exception e) {
                ctx.status(500).json(new Msg("reload failed: " + e.getMessage()));
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(metadataDao::close));
    }

    record Health(String status, String service, String version) {}
    record Msg(String message) {}
    record SearchResponse(String query, String mode, int page, int pageSize, int total,
                          java.util.List<SearchEngine.ScoredDoc> items) {}
}
