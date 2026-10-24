package com.example.directtest;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.FileProvider;

import java.io.File;

public class DiagnosticActivity extends AppCompatActivity implements DiagnosticLogger.LogListener {

    private TextView tvLogs;
    private ScrollView scrollView;
    private DiagnosticLogger logger;
    private boolean autoScroll = true;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_diagnostic);

        tvLogs = findViewById(R.id.tv_logs);
        scrollView = findViewById(R.id.scroll_view);

        Button btnClear = findViewById(R.id.btn_clear);
        Button btnSave = findViewById(R.id.btn_save);
        Button btnShare = findViewById(R.id.btn_share);
        Button btnExportCorr = findViewById(R.id.btn_export_corr);
        Button btnBack = findViewById(R.id.btn_back);

        logger = DiagnosticLogger.getInstance();

        // Загружаем существующие логи
        tvLogs.setText(logger.getLogsAsText());
        scrollToBottom();

        // Кнопки
        btnClear.setOnClickListener(v -> {
            logger.clear();
            tvLogs.setText("");
            Toast.makeText(this, "Логи очищены", Toast.LENGTH_SHORT).show();
        });

        btnSave.setOnClickListener(v -> saveToFile());

        btnShare.setOnClickListener(v -> shareLogs());

        // Журнал корреляции — то, по чему логи двух телефонов склеиваются
        btnExportCorr.setOnClickListener(v -> shareCorrelationLog());

        btnBack.setOnClickListener(v -> finish());

        // Авто-скролл по клику
        scrollView.setOnClickListener(v -> {
            autoScroll = !autoScroll;
            Toast.makeText(this,
                    autoScroll ? "Авто-скролл ВКЛ" : "Авто-скролл ВЫКЛ",
                    Toast.LENGTH_SHORT).show();
        });
    }

    @Override
    protected void onResume() {
        super.onResume();
        logger.addListener(this);
        // Обновляем логи
        tvLogs.setText(logger.getLogsAsText());
        scrollToBottom();
    }

    @Override
    protected void onPause() {
        super.onPause();
        logger.removeListener(this);
    }

    @Override
    public void onLogAdded(DiagnosticLogger.LogEntry entry) {
        tvLogs.append(entry.toString() + "\n");
        if (autoScroll) {
            scrollToBottom();
        }
    }

    private void scrollToBottom() {
        scrollView.post(() -> scrollView.fullScroll(View.FOCUS_DOWN));
    }

    private void saveToFile() {
        try {
            // Журнал корреляции пишется потоково; перед сохранением сбрасываем
            // буфер, чтобы файлы на двух телефонах были полными.
            com.example.directtest.fast.CorrelationLog.get().flush();
            File file = logger.saveToFile(this);
            Toast.makeText(this,
                    "Сохранено:\n" + file.getAbsolutePath(),
                    Toast.LENGTH_LONG).show();
        } catch (Exception e) {
            Toast.makeText(this,
                    "Ошибка сохранения: " + e.getMessage(),
                    Toast.LENGTH_LONG).show();
        }
    }

    /**
     * Поделиться журналом корреляции (TSV).
     *
     * <p>Этот файл — то, что нужно выгрузить с <b>обоих</b> телефонов: строки
     * сопоставляются по колонке {@code trace} (для сообщений это {@code msgId}),
     * а по {@code t_utc_ms} считается задержка.
     */
    private void shareCorrelationLog() {
        com.example.directtest.fast.CorrelationLog corr =
                com.example.directtest.fast.CorrelationLog.get();
        corr.flush();

        File file = corr.getFile();
        String text = corr.snapshot();

        if (file == null) {
            Toast.makeText(this, "Журнал корреляции недоступен", Toast.LENGTH_LONG).show();
            return;
        }

        Intent intent = new Intent(Intent.ACTION_SEND);
        intent.setType("text/plain");
        intent.putExtra(Intent.EXTRA_SUBJECT, "Ra-dio correlation " + corr.getDeviceId());
        intent.putExtra(Intent.EXTRA_TEXT, text);
        startActivity(Intent.createChooser(intent,
                "Журнал корреляции: " + file.getName()));
    }

    private void shareLogs() {
        try {
            File file = logger.saveToFile(this);

            Intent intent = new Intent(Intent.ACTION_SEND);
            intent.setType("text/plain");
            intent.putExtra(Intent.EXTRA_SUBJECT, "Diagnostic Logs");
            intent.putExtra(Intent.EXTRA_TEXT, logger.getLogsAsText());

            // Если нужно прикрепить файл:
            // Uri uri = FileProvider.getUriForFile(this,
            //     getPackageName() + ".fileprovider", file);
            // intent.putExtra(Intent.EXTRA_STREAM, uri);
            // intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);

            startActivity(Intent.createChooser(intent, "Поделиться логами"));
        } catch (Exception e) {
            Toast.makeText(this,
                    "Ошибка: " + e.getMessage(),
                    Toast.LENGTH_LONG).show();
        }
    }
}