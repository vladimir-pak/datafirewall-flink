package ru.gpb.datafirewall.rule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import ru.gpb.datafirewall.dto.Rule;

/**
 * Специализированный ClassLoader для загрузки классов бизнес-правил из байткода в памяти.
 *
 * <p>Загружает классы напрямую из переданной карты {@code имя_класса → байткод},
 * кеширует уже определённые классы и обеспечивает потокобезопасную загрузку.</p>
 *
 * <p>Используется вместе с {@link RulesReloader} для динамической загрузки и обновления
 * реализаций интерфейса {@link Rule} без перезапуска приложения.</p>
 */

public final class RuleClassLoader extends ClassLoader {

    private final Map<String, byte[]> bytecodeByName;
    private final ConcurrentMap<String, Class<?>> defined = new ConcurrentHashMap<>();

    public RuleClassLoader(Map<String, byte[]> bytecodeByName) {
        super(RuleClassLoader.class.getClassLoader());
        this.bytecodeByName = Map.copyOf(bytecodeByName);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Class<?> cached = defined.get(name);
        if (cached != null) return cached;

        synchronized (this) {
            cached = defined.get(name);
            if (cached != null) return cached;

            byte[] bytes = bytecodeByName.get(name);
            if (bytes == null) throw new ClassNotFoundException(name);

            Class<?> cls = defineClass(name, bytes, 0, bytes.length);
            defined.put(name, cls);
            resolveClass(cls);
            return cls;
        }
    }


    @SuppressWarnings("unchecked")
    public Class<? extends Rule> loadRule(String name) throws ClassNotFoundException {
        Class<?> cls = loadClass(name, true);

        if (!Rule.class.isAssignableFrom(cls)) {
            throw new ClassCastException(
                    "Loaded class " + name + " does not implement Rule"
            );
        }

        return (Class<? extends Rule>) cls;
    }
}

