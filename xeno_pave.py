import os
import cv2
import tqdm
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
from concurrent.futures import ThreadPoolExecutor, as_completed

def transfromimage(folder_input):
    
    matrix = np.asarray([[-4.72213304e-01,6.07375445e+00, -2.36383184e+01],
                        [ 9.15608405e-01,  2.65031461e+00, -7.51851357e+02],
                        [-2.53338772e-04,  4.22432334e-03,  1.00000000e+00]])
    
    angle = -90
    countc = os.cpu_count()
    cpu = countc / 2
    
    with ThreadPoolExecutor(max_workers=cpu) as executor:
        futures = [executor.submit(process_single_image, root, image_test, matrix, angle) for root, dirs, files in os.walk(folder_input) 
                   if 'PAVE-0' in root for image_test in tqdm.tqdm(files) if image_test.endswith('.jpg')]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing image: {e}")

def process_single_image(root, image_test, matrix, angle):
    path = os.path.join(root, image_test)
    img_2 = cv2.imread(path)
    img_2 = cv2.resize(img_2, (0,0), fx=0.5, fy=0.5)
    corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))
                    
    (h, w) = corrected_img.shape[:2]
    center = (w // 2, h // 2)
    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))
                    
    rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
    cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)
                   
    # plt.imshow(cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
    # plt.title('Corrected Image')
    # plt.axis('off')
    # plt.show()
                    
    if not os.path.exists(root):
        print('PAVE : Folder does not exist')
                    
    cv2.imwrite(os.path.join(root,image_test),cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))

folder_input = r'D:\xenomatix\output'
transfromimage(folder_input)