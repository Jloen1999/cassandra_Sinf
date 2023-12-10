package unex;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

/**
 * @author Jose Luis Obiang Ela Nanguang
 * @version 1.0
 */
public class RegistroPaquete {
    private UUID paqueteId;
    private String nombre;
    private UUID destinoId;
    private int duracion;
    private BigDecimal precio;
    private LocalDate fecha;

    public RegistroPaquete(UUID paqueteId, String nombre, UUID destinoId, int duracion, BigDecimal precio) {
        this.paqueteId = paqueteId;
        this.nombre = nombre;
        this.destinoId = destinoId;
        this.duracion = duracion;
        this.precio = precio;
    }

    public RegistroPaquete(UUID paqueteId) {
        this.paqueteId = paqueteId;
    }

    public RegistroPaquete(UUID paqueteId, String nombre, UUID destinoId, LocalDate fecha, int duracion, BigDecimal precio) {
        this.paqueteId = paqueteId;
        this.nombre = nombre;
        this.destinoId = destinoId;
        this.fecha = fecha;
        this.duracion = duracion;
        this.precio = precio;
    }

    public LocalDate getFecha() {
        return fecha;
    }

    public UUID getPaqueteId() {
        return paqueteId;
    }

    public String getNombre() {
        return nombre;
    }

    public UUID getDestinoId() {
        return destinoId;
    }

    public int getDuracion() {
        return duracion;
    }

    public BigDecimal getPrecio() {
        return precio;
    }
}
