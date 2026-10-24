package com.example.directtest.fast;

import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.location.LocationManager;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.provider.Settings;

import java.util.Locale;

/**
 * Проверки окружения под конкретные вендорские прошивки (D1–D5).
 *
 * <p>Главная причина «на Xiaomi/Redmi/vivo не находит устройства, хотя
 * разрешения выданы»: <b>выключен режим геолокации</b>. Документация Android
 * требует, чтобы Location Mode был включён для {@code discoverPeers},
 * {@code discoverServices} и {@code requestPeers} — само разрешение
 * {@code ACCESS_FINE_LOCATION} без включённой геолокации не даёт результата,
 * а список пиров приходит пустым и без ошибки. Поэтому проверяем это явно,
 * пишем в журнал и показываем пользователю кнопку «Открыть настройки».
 *
 * <p>Здесь же — детект возможностей (D4), чтобы не предлагать функции,
 * которых нет в прошивке, и подсказки по автозапуску (D2).
 */
public final class HwGuard {

    private HwGuard() {
    }

    /** Включён ли режим определения местоположения. */
    public static boolean isLocationEnabled(Context context) {
        if (context == null) return false;
        try {
            LocationManager lm = (LocationManager) context.getSystemService(Context.LOCATION_SERVICE);
            if (lm == null) return false;

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                return lm.isLocationEnabled();
            }
            // До API 28 отдельного метода нет: смотрим провайдеры
            boolean gps = lm.isProviderEnabled(LocationManager.GPS_PROVIDER);
            boolean network = lm.isProviderEnabled(LocationManager.NETWORK_PROVIDER);
            return gps || network;
        } catch (Exception e) {
            return false;
        }
    }

    /** Включён ли Wi-Fi. */
    public static boolean isWifiEnabled(Context context) {
        try {
            WifiManager wm = (WifiManager) context.getApplicationContext()
                    .getSystemService(Context.WIFI_SERVICE);
            return wm != null && wm.isWifiEnabled();
        } catch (Exception e) {
            return false;
        }
    }

    /** Поддерживает ли устройство Wi-Fi Direct (D4). */
    public static boolean isWifiDirectSupported(Context context) {
        if (context == null) return false;
        return context.getPackageManager()
                .hasSystemFeature(PackageManager.FEATURE_WIFI_DIRECT);
    }

    /** Текст проблемы для UI. {@code null}, если всё в порядке. */
    public static String environmentProblem(Context context) {
        if (!isWifiDirectSupported(context)) {
            return "Устройство не поддерживает Wi-Fi Direct";
        }
        if (!isWifiEnabled(context)) {
            return "Wi-Fi выключен — включите его";
        }
        if (!isLocationEnabled(context)) {
            return "Включите геолокацию (Location). Без неё Android не отдаёт "
                    + "список Wi-Fi Direct устройств, даже если разрешения выданы";
        }
        return null;
    }

    /** Открыть системные настройки геолокации. */
    public static void openLocationSettings(Context context) {
        try {
            context.startActivity(new Intent(Settings.ACTION_LOCATION_SOURCE_SETTINGS)
                    .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK));
        } catch (Exception ignored) {
        }
    }

    /**
     * Подсказка по автозапуску для вендорской прошивки (D2).
     *
     * <p>Даже foreground service с Wi-Fi lock убивается агрессивным
     * энергосбережением MIUI/HyperOS, Funtouch/OriginOS, ColorOS, realme UI —
     * и «рация» замолкает именно на этих телефонах.
     */
    public static String autostartHint() {
        String brand = Build.MANUFACTURER != null
                ? Build.MANUFACTURER.toLowerCase(Locale.US) : "";

        if (brand.contains("xiaomi") || brand.contains("redmi") || brand.contains("poco")) {
            return "MIUI/HyperOS: Настройки → Приложения → Ra-dio → Автозапуск (вкл), "
                    + "Энергосбережение → «Без ограничений», плюс закрепите приложение в списке задач";
        }
        if (brand.contains("vivo") || brand.contains("iqoo")) {
            return "Funtouch/OriginOS: Настройки → Батарея → Фоновое энергопотребление → "
                    + "разрешить для Ra-dio; там же «Автозапуск»";
        }
        if (brand.contains("oppo") || brand.contains("realme") || brand.contains("oneplus")) {
            return "ColorOS/realme UI: Настройки → Батарея → Оптимизация батареи → "
                    + "Ra-dio → «Не оптимизировать»; Автозапуск → включить";
        }
        if (brand.contains("huawei") || brand.contains("honor")) {
            return "EMUI/MagicOS: Настройки → Батарея → Запуск приложений → "
                    + "Ra-dio → «Управлять вручную» (все три пункта)";
        }
        if (brand.contains("samsung")) {
            return "One UI: Настройки → Батарея → Ограничения фоновой работы → "
                    + "убрать Ra-dio из «Спящих приложений»";
        }
        return "Разрешите Ra-dio работать в фоне без ограничений батареи "
                + "в настройках системы";
    }

    /** Одна строка для журнала корреляции: всё, что влияет на обнаружение. */
    public static String describe(Context context) {
        return "model=" + Build.MANUFACTURER + "_" + Build.MODEL
                + "|api=" + Build.VERSION.SDK_INT
                + "|p2p=" + (isWifiDirectSupported(context) ? 1 : 0)
                + "|wifi=" + (isWifiEnabled(context) ? 1 : 0)
                + "|loc=" + (isLocationEnabled(context) ? 1 : 0)
                + "|persistent=" + (supportsPersistentGroup() ? 1 : 0)
                + "|band=" + (supportsBandSelection() ? 1 : 0);
    }

    /** Persistent group доступна с API 29 (D/A6). */
    public static boolean supportsPersistentGroup() {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q;
    }

    /** Выбор диапазона группы — тоже с API 29. */
    public static boolean supportsBandSelection() {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q;
    }

    /** Явный выбор диапазона Wi-Fi Direct (D5): 2,4 ГГц по умолчанию. */
    public static int preferredBand() {
        return P2pBand.classify(P2pBand.PREFERRED);
    }

    /** Вспомогательный класс без зависимостей от android.net.wifi.p2p. */
    public static final class P2pBand {
        public static final int AUTO = 0;
        public static final int BAND_24 = 1;
        public static final int BAND_5 = 2;
        /** 2,4 ГГц: дальше и стабильнее на бюджетных чипсетах. */
        public static final int PREFERRED = BAND_24;

        private P2pBand() {
        }

        static int classify(int band) {
            return band;
        }
    }
}
