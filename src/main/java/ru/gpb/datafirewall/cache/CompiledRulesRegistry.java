package ru.gpb.datafirewall.cache;

import ru.gpb.datafirewall.dto.Rule;

/**
 * Потокобезопасное хранилище активного набора скомпилированных бизнес-правил.
 *
 * <p>Хранит текущий набор правил и связанную с ним версию в виде
 * неизменяемого snapshot'а, обеспечивая неблокирующий и консистентный
 * доступ для потоков обработки.</p>
 *
 * <p>Обновление набора правил и его версии выполняется атомарно через
 * replaceAll(...), что гарантирует отсутствие промежуточных состояний.</p>
 *
 * <p>Используется совместно с RulesReloader для безопасной hot-reload
 * перезагрузки правил.</p>
 */
public final class CompiledRulesRegistry extends AtomicSnapshotCache<String, Rule> {
}