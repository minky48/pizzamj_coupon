package pizzamj;

import pizzamj.config.kafka.KafkaProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Optional;

@Service
public class PolicyHandler{

    @Autowired
    CouponRepository couponRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void onStringEventListener(@Payload String eventString){

    }

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverDelivered_PublishCoupon(@Payload Delivered delivered){

        if(delivered.isMe()){
            if(delivered.getDeliveryStatus().equals("Finished")){
                Coupon coupon = new Coupon();
                coupon.setOrderId(delivered.getOrderId());
                coupon.setStatus("published");
                couponRepository.save(coupon);
            }

            System.out.println("##### listener PublishCoupon : " + delivered.toJson());
        }
    }
    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverRefunded_PublishCoupon(@Payload Refunded refunded){

        if(refunded.isMe()){

            int flag=0;
            Iterator<Coupon> iterator = couponRepository.findAll().iterator();
            while(iterator.hasNext()){
                Coupon pointTmp = iterator.next();
                if(pointTmp.getOrderId() == refunded.getOrderId()){
                    Optional<Coupon> PointOptional = couponRepository.findById(pointTmp.getId());
                    Coupon coupon = PointOptional.get();
                    coupon.setStatus("couponRefunded");
                    couponRepository.save(coupon);
                    flag=1;
                }
            }

            if (flag==0 ){
                Coupon coupon = new Coupon();
                coupon.setOrderId(refunded.getOrderId());
                coupon.setStatus("couponRefunded");
                couponRepository.save(coupon);
            }


            System.out.println("##### listener PublishCoupon : " + refunded.toJson());
        }
    }

}
