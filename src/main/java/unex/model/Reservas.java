package unex.model;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;

import java.util.UUID;

@Entity
public class Reservas {
    @PartitionKey
    private UUID ReservasId;
    private UUID paqueteId;
    private UUID clienteId;
    private String fechaInicio;
    private String fechaFin;
    private boolean pagado;

    // Atributos adicionales para establecer relaciones
    private Paquetes paqueteTuristico;
    private Clientes cliente;

    // Constructor, getters y setters

    public Reservas() {
        // Constructor vacío requerido por el mapeador de objetos
    }

    public Reservas(UUID ReservasId, UUID paqueteId, UUID clienteId, String fechaInicio, String fechaFin, boolean pagado) {
        this.ReservasId = ReservasId;
        this.paqueteId = paqueteId;
        this.clienteId = clienteId;
        this.fechaInicio = fechaInicio;
        this.fechaFin = fechaFin;
        this.pagado = pagado;
    }

    // Resto del código...

    public Paquetes getPaqueteTuristico() {
        return paqueteTuristico;
    }

    public void setPaqueteTuristico(Paquetes paqueteTuristico) {
        this.paqueteTuristico = paqueteTuristico;
    }

    public Clientes getCliente() {
        return cliente;
    }

    public void setCliente(Clientes cliente) {
        this.cliente = cliente;
    }
}
